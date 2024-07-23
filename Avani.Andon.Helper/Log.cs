using System;
using System.IO;

namespace Avani.Helper
{
    /// <summary>
    /// Cung cấp tính năng ghi logs xuống máy local
    /// </summary>
    public partial class Log
    {
        #region properties
        /// <summary>
        /// Đường dẫn đầy đủ của thư mục chứa logs
        /// </summary>
        public string LogFolder { get; set; }
        /// <summary>
        /// Mức ghi nhận log
        /// </summary>
        public LogType Level { get; set; }
        #endregion
        #region constructors
        public Log(string logFolder)
        {
            this.LogFolder = logFolder;
            this.Level = LogType.Info;
            try
            {
                if(!Directory.Exists(this.LogFolder))
                {
                    Directory.CreateDirectory(this.LogFolder);
                }
            }
            catch
            {

            }
        }
        #endregion
        #region public methods
        /// <summary>
        /// Ghi log
        /// </summary>
        /// <param name="category">Nhóm thông tin</param>
        /// <param name="message">Thông điệp log</param>
        /// <param name="type">Loại log</param>
        public void Write(string category, string message, LogType type, string additionFileName = "")
        {
            Write(category, "System", message, type, additionFileName);
        }
        /// <summary>
        /// Ghi log lỗi
        /// </summary>
        /// <param name="category">Nhóm thông tin</param>
        /// <param name="ex">Exception lỗi</param>
        public void Write(string category, Exception ex)
        {
            Write(category, "System", ex);
        }
        /// <summary>
        /// Ghi log lỗi
        /// </summary>
        /// <param name="category">Nhóm thông tin</param>
        /// <param name="threadId">Định danh tiến trình</param>
        /// <param name="ex">Exception lỗi</param>
        public void Write(string category, string threadId, Exception ex)
        {
            Write(category, threadId, ex.ToString(), LogType.Error);
        }
        /// <summary>
        /// Ghi log
        /// </summary>
        /// <param name="category">Nhóm thông tin</param>
        /// <param name="threadId">Định danh tiến trình</param>
        /// <param name="message">Thông điệp log</param>
        /// <param name="type">Loại log</param>
        public void Write(string category, string threadId, string message, LogType type, string additionFileName = "")
        {
            if (type > this.Level) return;
            if (string.IsNullOrEmpty(this.LogFolder)) return;
            try
            {
                string _additionFileName = additionFileName;
                if (_additionFileName != "")
                {
                    _additionFileName = "_" + _additionFileName;
                }

                string _folder = Path.Combine(this.LogFolder,DateTime.Now.ToString("yyyyMM"));
                if (!Directory.Exists(_folder))
                {
                    Directory.CreateDirectory(_folder);
                }

                string fileName = Path.Combine(_folder, DateTime.Now.ToString("yyyy-MM-dd") + _additionFileName + ".txt");
                string logInfo = $"{type.GetDescription()} - {DateTime.Now:HH:mm:ss} {message}";
                if(File.Exists(fileName))
                {
                    using (StreamWriter writer = File.AppendText(fileName))
                    {
                        writer.WriteLine(logInfo);
                    }
                }
                else
                {
                    using (StreamWriter writer = File.CreateText(fileName))
                    {
                        writer.WriteLine(logInfo);
                    }
                }
            }
            catch
            {

            }
        }
        #endregion
        #region private methods
        #endregion
    }
    /// <summary>
    /// Danh mục loại thông tin log
    /// </summary>
    public enum LogType
    {
        [System.ComponentModel.Description("Error")]
        Error = 0,
        [System.ComponentModel.Description("Warning")]
        Warning = 1,
        [System.ComponentModel.Description("Info")]
        Info = 2,
        [System.ComponentModel.Description("Debug")]
        Debug = 3
    }
}
