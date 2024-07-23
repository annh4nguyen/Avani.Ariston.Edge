namespace Avani.Helper
{
    /// <summary>
    /// Cung cấp các method mở rộng của kiểu Stream
    /// </summary>
    public static partial class StreamExtension
    {
        /// <summary>
        /// Convert Stream to byte[]
        /// </summary>
        /// <param name="inputStream">Stream cần convert</param>
        /// <param name="bufferSize">buffer size (bytes)</param>
        /// <returns>Mảng byte cần trả về</returns>
        public static byte[] ToBytes(this System.IO.Stream inputStream, int bufferSize)
        {
            byte[] buffer = new byte[bufferSize];
            using (System.IO.MemoryStream ms = new System.IO.MemoryStream())
            {
                int read;
                while ((read = inputStream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    ms.Write(buffer, 0, read);
                }
                return ms.ToArray();
            }
        }
        /// <summary>
        /// Convert Stream to byte[]
        /// buffer size mặc định là 10Kb
        /// </summary>
        /// <param name="inputStream">Stream cần convert</param>
        /// <returns>Mảng byte cần trả về</returns>
        public static byte[] ToBytes(this System.IO.Stream inputStream)
        {
            int bufferSize = 10 * 1024;
            return inputStream.ToBytes(bufferSize);
        }
    }
}
