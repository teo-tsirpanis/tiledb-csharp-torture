using TileDB.CSharp;
using TileDBArray = TileDB.CSharp.Array;
using TileDBAttribute = TileDB.CSharp.Attribute;

// TileDB array data type which reflects an unsigned 16-bit integer pixel type
using AttrType = System.UInt16;
using System.Text;

// Tile size in the X dimension
const int SizeX = 1000;

// Tile size in the Y dimension
const int SizeY = 1000;

// Number of timepoints
const int SizeT = 1;

// Number of channels
const int SizeC = 20;

// Number of sections/slices in the axial plane
const int SizeZ = 1;

// Height of the image
const int TileSizeY = 22000;

// Width of the image
const int TileSizeX = 13000;

// Number of bytes in memory that a single pixel (TileDB cell) occupies
const int BytesPerPixel = sizeof(AttrType);

// Whether the data is of 3 channel unsigned 8-bit integer pixel type
const bool Rgb = false;

string TileDBRoot = Path.Join(Path.GetTempPath(), "tiledb-csharp-torture");

// Number of parallel workers that will be reading and writing from the
// various TileDB arrays.
const int MaxWorkers = 4;

// Number of resolutions we will process
const int Resolutions = 2;

// Tile to tile random overlap bounds which will used to simulate real
// world non-adjacent writes.
const int Overlap = 100;

const bool DoConsolidate = true;

ThreadPool.SetMaxThreads(MaxWorkers, MaxWorkers);

Directory.Delete(TileDBRoot, true);
Directory.CreateDirectory(TileDBRoot);

Console.WriteLine($"TileDB root: {TileDBRoot}");

using Context ctx = new();

CalculateResolutionZero();

static string FormatSubarray(Subarray s)
{
    StringBuilder sb = new();
    sb.Append('[');
    for (uint i = 0; i < 5; i++)
    {
        if (i > 0)
            sb.Append(", ");
        var (Start, End) = s.GetRange<int>(i, 0);
        sb.Append($"{Start}:{End}");
    }
    sb.Append(']');
    return sb.ToString();
}

string CreateArray(int resolution, int extentY, int extentX)
{
    using var t = Dimension.Create(ctx, "t", 0, SizeT - 1, 1);
    using var c = Dimension.Create(ctx, "c", 0, SizeC - 1, 1);
    using var z = Dimension.Create(ctx, "z", 0, SizeZ - 1, 1);
    using var y = Dimension.Create(ctx, "y", 0, TileSizeY - 1, extentY);
    using var x = Dimension.Create(ctx, "x", 0, TileSizeX - 1, extentX);
    using var domain = new Domain(ctx);
    domain.AddDimensions(t, c, z, y, x);

    using var filterList = new FilterList(ctx);
    using var filter = new Filter(ctx, FilterType.Zstandard);
    filterList.AddFilter(filter);

    using var a1 = TileDBAttribute.Create<AttrType>(ctx, "a1");
    a1.SetFilterList(filterList);
    a1.SetFillValue<AttrType>(Rgb ? 0xFF : 0);

    using var schema = new ArraySchema(ctx, ArrayType.Dense);
    schema.SetDomain(domain);
    schema.AddAttribute(a1);

    string arrayUri = $"{TileDBRoot}/torture_{resolution}_{extentY}_{extentX}";
    Console.WriteLine($"Creating array: {arrayUri}");
    TileDBArray.Create(ctx, arrayUri, schema);
    return arrayUri;
}

void WriteImage(TileDBArray array, int t, int c, int z, int y0, int x0)
{
    int y1 = Math.Min(y0 + TileSizeY - 1, SizeY - 1);
    int x1 = Math.Min(x0 + TileSizeX - 1, SizeX - 1);
    int area = (y1 + 1 - y0) * (x1 + 1 - x0);

    AttrType[] data = new AttrType[area];
    using Query q = new(array, QueryType.Write);
    using Subarray s = new(array);
    q.SetLayout(LayoutType.RowMajor);
    s.AddRange(0, t, t);
    s.AddRange(1, c, c);
    s.AddRange(2, z, z);
    s.AddRange(3, y0, y1);
    s.AddRange(4, x0, x1);
    q.SetSubarray(s);
    q.SetDataBuffer("a1", data);
    q.Submit();
}

void Consolidate(string uri)
{
    if (!DoConsolidate)
    {
        Console.WriteLine($"Not consolidating {uri}");
        return;
    }
    using var config = new Config();
    config.Set("sm.consolidation.mode", "fragment_meta");
    config.Set("sm.consolidation.step_min_frag", "0");
    TileDBArray.Consolidate(ctx, uri, config);
    TileDBArray.Vacuum(ctx, uri, config);
}

void CalculateResolutionZero()
{
    string uri = CreateArray(0, TileSizeY, TileSizeX);
    using (var array = new TileDBArray(ctx, uri))
    {
        array.Open(QueryType.Write);
        for (int t = 0; t < SizeT; t++)
        {
            for (int c = 0; c < SizeC; c++)
            {
                for (int z = 0; z < SizeZ; z++)
                {
                    Console.WriteLine($"Calculate resolution for t={t}, c={c}, z={z}");
                    var tasks = new List<Task>();
                    int gridSizeY = (int)double.Ceiling((double)TileSizeY / TileSizeY);
                    int gridSizeX = (int)double.Ceiling((double)TileSizeX / TileSizeX);
                    for (int y = 0; y < gridSizeY; y++)
                    {
                        for (int x = 0; x < gridSizeX; x++)
                        {
                            int tileY = y * TileSizeY + Random.Shared.Next(Overlap) + 1;
                            int tileX = x * TileSizeX + Random.Shared.Next(Overlap) + 1;
                            tasks.Add(Task.Run(() => WriteImage(array, t, c, z, tileY, tileX)));
                        }
                    }
                    Task.WaitAll(tasks.ToArray());
                }
            }
        }
    }
    Consolidate(uri);
}
