using System;

namespace Avani.Helper
{
    /// <summary>
    /// Cung cấp các method mở rộng của string
    /// </summary>
    public static partial class StringExtensions
    {
        /// <summary>
        /// Chia chuỗi thành mảng theo độ dài
        /// </summary>
        /// <param name="value">Chuỗi cần chia</param>
        /// <param name="size">Độ dài phần tử</param>
        /// <returns>Mảng chuỗi kết quả</returns>
        public static string[] Split(this string value, int size)
        {
            if (size <= 0 || string.IsNullOrEmpty(value)) return new string[] { value };
            int length = value.Length;
            int count = (length + size - 1) / size;
            string[] result = new string[count];
            for (int i = 0; i < count; ++i)
            {
                result[i] = value.Substring(i * size, Math.Min(size, length));
                length -= size;
            }
            return result;
        }
    }
}
