using System.Collections.Generic;
using System.Linq;

namespace Avani.Helper
{
    /// <summary>
    /// Cung cấp các method mở rộng của mảng
    /// </summary>
    public static partial class ArrayExtension
    {
        /// <summary>
        /// Chia mảng byte thành các mảng con theo size
        /// </summary>
        /// <param name="value">Mảng byte cần chia</param>
        /// <param name="size">size cần chia</param>
        /// <returns>Các mảng byte trả về</returns>
        public static IEnumerable<byte[]> Split(this byte[] value, int size)
        {
            if (size <= 0 || value.Length == 0) yield return value;
            int count = (value.Length + size - 1) / size;
            for (int i = 0; i < count; i++)
            {
                yield return value.Skip(i * size).Take(size).ToArray();
            }
        }
    }
}
