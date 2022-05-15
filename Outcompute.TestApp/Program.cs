using Outcompute.ColumnStore;
using Serilog;
using XPTO;

var logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();

var cs = new ColumnStore<Book>(new ColumnStoreOptions { RowGroupSizeThreshold = 5 });

cs.Add(new Book { Year = 1, Title = "A" });
PrintStats();

cs.Add(new Book { Year = 1, Title = "B" });
PrintStats();

cs.Add(new Book { Year = 1, Title = "C" });
PrintStats();

cs.Add(new Book { Year = 2, Title = "A" });
PrintStats();

cs.Add(new Book { Year = 2, Title = "A" });
PrintStats();

cs.Add(new Book { Year = 3, Title = "A" });
PrintStats();

cs.Add(new Book { Year = 3, Title = "B" });
PrintStats();

cs.Add(new Book { Year = 3, Title = "C" });
PrintStats();

cs.Add(new Book { Year = 3, Title = "C" });
PrintStats();

cs.Add(new Book { Year = 3, Title = "C" });
PrintStats();

void PrintStats()
{
    foreach (var item in cs)
    {
        logger.Information("{@Item}", item);
    }
    logger.Information("{@Stats}", cs.GetStats());
    logger.Information("");
}

namespace XPTO
{
    [ColumnStore]
    public record struct Book()
    {
        [ColumnStoreProperty]
        public int Year { get; set; } = 0;

        [ColumnStoreProperty]
        public string Title { get; set; } = "";
    }
}