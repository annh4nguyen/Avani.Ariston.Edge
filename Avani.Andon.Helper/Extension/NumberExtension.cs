using System;

namespace Avani.Helper
{
    /// <summary>
    /// Cung cấp các method mở rộng của kiểu Number
    /// </summary>
    public static partial class NumberExtension
    {
        /// <summary>
        /// Convert định dạng filesize
        /// </summary>
        /// <param name="value">Giá trị cần convert</param>
        /// <param name="decimalPlace">Phần thập phân cần lấy</param>
        /// <returns>Chuỗi định dạng filesize trả về</returns>
        public static string ToFileSize(this int value, int decimalPlace)
        {
            return Convert.ToDouble(value).ToFileSize(decimalPlace);
        }
        /// <summary>
        /// Convert định dạng filesize
        /// Phần thập phân mặc định là 1
        /// </summary>
        /// <param name="value">Giá trị cần convert</param>
        /// <returns>Chuỗi định dạng filesize trả về</returns>
        public static string ToFileSize(this int value)
        {
            return value.ToFileSize(1);
        }
        /// <summary>
        /// Convert định dạng filesize
        /// </summary>
        /// <param name="value">Giá trị cần convert</param>
        /// <param name="decimalPlace">Phần thập phân cần lấy</param>
        /// <returns>Chuỗi định dạng filesize trả về</returns>
        public static string ToFileSize(this long value, int decimalPlace)
        {
            return Convert.ToDouble(value).ToFileSize(decimalPlace);
        }
        /// <summary>
        /// Convert định dạng filesize
        /// Phần thập phân mặc định là 1
        /// </summary>
        /// <param name="value">Giá trị cần convert</param>
        /// <returns>Chuỗi định dạng filesize trả về</returns>
        public static string ToFileSize(this long value)
        {
            return value.ToFileSize(1);
        }
        /// <summary>
        /// Convert định dạng filesize
        /// </summary>
        /// <param name="value">Giá trị cần convert</param>
        /// <param name="decimalPlace">Phần thập phân cần lấy</param>
        /// <returns>Chuỗi định dạng filesize trả về</returns>
        public static string ToFileSize(this double value, int decimalPlace)
        {
            string[] suffixes = { "bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB" };
            string IFormat = "#,##0";
            if (decimalPlace > 0) IFormat += "." + new string('0', decimalPlace);
            for (int i = 0; i < suffixes.Length; i++)
            {
                if (value <= (Math.Pow(1024, i + 1)))
                {
                    return string.Format("{0} {1}", (value / Math.Pow(1024, i)).ToString(IFormat), suffixes[i]);
                }
            }
            return string.Format("{0} {1}", (value / Math.Pow(1024, suffixes.Length - 1)).ToString(IFormat), suffixes[suffixes.Length - 1]);
        }
        /// <summary>
        /// Convert định dạng filesize
        /// Phần thập phân mặc định là 1
        /// </summary>
        /// <param name="value">Giá trị cần convert</param>
        /// <returns>Chuỗi định dạng filesize trả về</returns>
        public static string ToFileSize(this double value)
        {
            return value.ToFileSize();
        }
    }
}
