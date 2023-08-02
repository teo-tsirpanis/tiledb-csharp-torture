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
CalculatePyramid();

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

    string arrayUri = $"{TileDBRoot}/torture_{resolution}";
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

AttrType[] DownsampleTileSis(AttrType[] sourceBuffer, int sourceWidth, int sourceHeight)
{
    // In the Java code downsampling needed an external library.
    // We will just create a dummy array.
    return new AttrType[sourceWidth * sourceHeight];
}

void ProcessTile(TileDBArray source, TileDBArray destination, int resolution, int t, int c, int z, int y0, int x0)
{
    int factor = (int)Math.Pow(2, resolution);
    int previousFactor = (int)Math.Pow(2, resolution - 1);
    int sizeY = SizeY / factor;
    int sizeX = SizeX / factor;
    int sourceY0 = y0 * 2;
    int sourceY1 = int.Min(sourceY0 + TileSizeY * 2, SizeY / previousFactor) - 1;
    int sourceX0 = x0 * 2;
    int sourceΧ1 = int.Min(sourceX0 + TileSizeX * 2, SizeX / previousFactor) - 1;
    int sourceWidth = sourceΧ1 + 1 - sourceX0;
    int sourceHeight = sourceY1 + 1 - sourceY0;
    int y1 = int.Min(y0 + sourceHeight / 2, SizeY) - 1;
    int x1 = int.Min(x0 + sourceWidth / 2, SizeX) - 1;

    AttrType[] data = new AttrType[sourceHeight * sourceWidth];
    using var sourceQuery = new Query(source, QueryType.Read);
    using var destinationQuery = new Query(destination, QueryType.Write);
    using var sourceSubarray = new Subarray(source);
    using var destinationSubarray = new Subarray(destination);

    sourceSubarray.AddRange(0, t, t);
    sourceSubarray.AddRange(1, c, c);
    sourceSubarray.AddRange(2, z, z);
    sourceSubarray.AddRange(3, sourceY0, sourceY1);
    sourceSubarray.AddRange(4, sourceX0, sourceΧ1);
    sourceQuery.SetSubarray(sourceSubarray);
    sourceQuery.SetDataBuffer("a1", data);
    sourceQuery.Submit();
    Console.WriteLine($"Read rectangle: {FormatSubarray(sourceSubarray)}; status: {sourceQuery.Status()}");

    AttrType[] destinationData = DownsampleTileSis(data, sourceWidth, sourceHeight);

    destinationSubarray.AddRange(0, t, t);
    destinationSubarray.AddRange(1, c, c);
    destinationSubarray.AddRange(2, z, z);
    destinationSubarray.AddRange(3, sourceY0, sourceY1);
    destinationSubarray.AddRange(4, sourceX0, sourceΧ1);
    destinationQuery.SetSubarray(destinationSubarray);
    destinationQuery.SetDataBuffer("a1", destinationData);
    destinationQuery.Submit();
    Console.WriteLine($"Read rectangle: {FormatSubarray(destinationSubarray)}; status: {destinationQuery.Status()}");
}

void CalculatePyramid()
{
    for (int resolution = 1; resolution < Resolutions; resolution++)
    {
        string uri = CreateArray(resolution, TileSizeY, TileSizeX);
        for (int t = 0; t < SizeT; t++)
        {
            for (int c = 0; c < SizeC; c++)
            {
                for (int z = 0; z < SizeZ; z++)
                {
                    Console.WriteLine($"Calculate pyramid for Resolution:{resolution} T:{t} C:{c} Z:{z}");
                    var sourceRoot = $"{TileDBRoot}/torture_{resolution - 1}";
                    var destinationRoot = $"{TileDBRoot}/torture_{resolution}";

                    using var source = new TileDBArray(ctx, sourceRoot);
                    using var destination = new TileDBArray(ctx, destinationRoot);
                    source.Open(QueryType.Read);
                    destination.Open(QueryType.Write);
                    List<Task> tasks = new();
                    int gridSizeY = (int)double.Ceiling((double)SizeY / 2 / TileSizeY);
                    int gridSizeX = (int)double.Ceiling((double)SizeX / 2 / TileSizeX);
                    for (int y = 0; y < gridSizeY; y++)
                    {
                        for (int x = 0; x < gridSizeX; x++)
                        {
                            int tileY = y * TileSizeY;
                            int tileX = x * TileSizeX;
                            tasks.Add(Task.Run(() => ProcessTile(source, destination, resolution, t, c, z, tileY, tileX)));
                        }
                    }
                    Task.WaitAll(tasks.ToArray());
                }
            }
        }
        Consolidate(uri);
    }
}
