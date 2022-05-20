using System.Text;

namespace Outcompute.ColumnStore.CodeGenerator;

internal static class HelperExtensions
{
    public static string Render<T>(this IEnumerable<T> items, Func<T, string> render, string separator = "\r\n")
    {
        var builder = new StringBuilder();

        foreach (var item in items)
        {
            if (builder.Length > 0)
            {
                builder.Append(separator);
            }

            builder.Append(render(item));
        }

        return builder.ToString();
    }
}