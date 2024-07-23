using System;
using System.Collections.Generic;
using System.Linq;

namespace Avani.Helper
{
    /// <summary>
    /// Cung cấp các method mở rộng của kiểu DateTime
    /// </summary>
    public static partial class DateTimeExtension
    {
        /// <summary>
        /// Tính Ngày làm việc tiếp theo
        /// </summary>
        /// <param name="date">Ngày bắt đầu</param>
        /// <param name="days">Số ngày làm việc tiếp theo</param>
        /// <param name="holidays">Danh sách ngày nghỉ</param>
        /// <returns>Ngày làm việc tiếp theo sau days ngày làm việc</returns>
        public static DateTime Workday(this DateTime date, long days, IEnumerable<DateTime> holidays)
        {
            DateTime targetDate = date.AddDays(days + (days / 7) * 2);
            long businessDays = date.NetworkDays(targetDate, holidays) - 1;
            while (businessDays < days || !targetDate.IsWorkday(holidays))
            {
                targetDate = targetDate.AddDays(1);
                if (targetDate.IsWorkday(holidays)) businessDays++;
            }
            return targetDate;
        }
        /// <summary>
        /// Tính Ngày làm việc tiếp theo
        /// </summary>
        /// <param name="date">Ngày bắt đầu</param>
        /// <param name="days">Số ngày làm việc tiếp theo</param>
        /// <returns>Ngày làm việc tiếp theo sau days ngày làm việc</returns>
        public static DateTime Workday(this DateTime date, long days)
        {
            DateTime targetDate = date.AddDays(days + (days / 7) * 2);
            long businessDays = date.NetworkDays(targetDate) - 1;
            while (businessDays < days || !targetDate.IsWorkday())
            {
                targetDate = targetDate.AddDays(1);
                if (targetDate.IsWorkday()) businessDays++;
            }
            return targetDate;
        }
        /// <summary>
        /// Tính số ngày làm việc giữa 2 ngày
        /// </summary>
        /// <param name="date">Ngày bắt đầu</param>
        /// <param name="otherDate">Ngày khác</param>
        /// <param name="holidays">Danh sách ngày nghỉ</param>
        /// <returns>Số ngày làm việc giữa 2 ngày</returns>
        public static long NetworkDays(this DateTime date, DateTime otherDate, IEnumerable<DateTime> holidays)
        {
            DateTime smallDate = date <= otherDate ? date.Date : otherDate.Date;
            DateTime bigDate = date <= otherDate ? otherDate.Date : date.Date;
            long workdays = smallDate.NetworkDays(otherDate);
            if (holidays != null) workdays -= holidays.Count(d => d.Date >= smallDate && d.Date <= bigDate && d.IsWorkday());
            return workdays;
        }
        /// <summary>
        /// Tính số ngày làm việc giữa 2 ngày
        /// </summary>
        /// <param name="date">Ngày bắt đầu</param>
        /// <param name="otherDate">Ngày khác</param>
        /// <returns>Số ngày làm việc giữa 2 ngày</returns>
        public static long NetworkDays(this DateTime date, DateTime otherDate)
        {
            DateTime smallDate = date <= otherDate ? date.Date : otherDate.Date;
            int prefixDays;
            if (smallDate.DayOfWeek == DayOfWeek.Saturday)
            {
                smallDate = smallDate.AddDays(-5);
                prefixDays = 5;
            }
            else if (smallDate.DayOfWeek == DayOfWeek.Sunday)
            {
                smallDate = smallDate.AddDays(-6);
                prefixDays = 5;
            }
            else
            {
                prefixDays = (int)smallDate.DayOfWeek - 1;
                smallDate = smallDate.AddDays(-prefixDays);
            }
            DateTime bigDate = date <= otherDate ? otherDate.Date : date.Date;
            int sufixDays;
            if (bigDate.DayOfWeek == DayOfWeek.Saturday)
            {
                bigDate = bigDate.AddDays(1);
                sufixDays = 1;
            }
            else if (bigDate.DayOfWeek == DayOfWeek.Sunday)
            {
                sufixDays = 1;
            }
            else
            {
                sufixDays = 6 - (int)bigDate.DayOfWeek;
                bigDate = bigDate.AddDays(sufixDays + 1);
            }
            return (Convert.ToInt64((bigDate - smallDate).TotalDays + 1) / 7) * 5 - prefixDays - sufixDays + 1;
        }
        /// <summary>
        /// Kiểm tra ngày làm việc (Ngày nghỉ trong tuần bao gồm Thứ 7 và Chủ nhật)
        /// </summary>
        /// <param name="date">Ngày cần kiểm tra</param>
        /// <param name="holidays">Danh sách ngày nghỉ</param>
        /// <returns>True: Ngày làm việc, False: Ngày nghỉ</returns>
        public static bool IsWorkday(this DateTime date, IEnumerable<DateTime> holidays)
        {
            if (holidays != null && holidays.Any(d => d.Date == date.Date)) return false;
            return date.IsWorkday();
        }
        /// <summary>
        /// Kiểm tra ngày làm việc (Ngày nghỉ trong tuần bao gồm Thứ 7 và Chủ nhật)
        /// </summary>
        /// <param name="date">Ngày cần kiểm tra</param>
        /// <returns>True: Ngày làm việc, False: Ngày nghỉ</returns>
        public static bool IsWorkday(this DateTime date)
        {
            return date.DayOfWeek != DayOfWeek.Saturday && date.DayOfWeek != DayOfWeek.Sunday;
        }
        /// <summary>
        /// Ngày tiếp theo gần nhất có Thứ bằng với thứ của tham số 'date'
        /// </summary>
        /// <param name="date">Ngày cần tính</param>
        /// <param name="weekday">Thứ trong tuần</param>
        /// <returns>Ngày trả về kiểu DateTime</returns>
        /// Ex: 
        ///     Ngày Thứ 2 tiếp theo của ngày 2013/01/01 => 2013/01/07
        ///     Ngày Thứ 3 tiếp theo của ngày 2013/01/01 => 2013/01/08
        ///     Ngày Thứ 4 tiếp theo của ngày 2013/01/01 => 2013/01/02
        public static DateTime NextWeekday(this DateTime date, DayOfWeek weekday)
        {
            DateTime outputDate = date.AddDays(weekday - date.DayOfWeek);
            if (outputDate <= date)
            {
                outputDate = outputDate.AddDays(7);
            }
            return outputDate;
        }

        /// <summary>
        /// Ngày trong tháng tiếp theo
        /// </summary>
        /// <param name="date">Ngày cần tính</param>
        /// <param name="dayOfMonth">Ngày trong tháng</param>
        /// <returns>Ngày trả về kiểu DateTime</returns>
        /// Ex: 
        ///     Ngày mùng 3 tiếp theo của ngày 2013/01/15 => 2013/02/03
        ///     Ngày 20 tiếp theo của ngày 2013/01/15 => 2013/01/20
        public static DateTime NextDayOfMonth(this DateTime date, int dayOfMonth)
        {
            DateTime outputDate = date;
            if (outputDate.Day < dayOfMonth)
            {
                outputDate = outputDate.AddDays(dayOfMonth - outputDate.Day);
            }
            else
            {
                DateTime nextDate = new DateTime(outputDate.Year, outputDate.Month + 1, dayOfMonth);
                if (nextDate.Day == dayOfMonth)
                {
                    outputDate = nextDate;
                }
                else
                {
                    outputDate = nextDate.AddDays(-nextDate.Day);
                }
            }
            return outputDate;
        }

        /// <summary>
        /// Thứ trong tháng tiếp theo
        /// </summary>
        /// <param name="date">Ngày cần tính</param>
        /// <param name="weekday">Thứ trong tuần</param>
        /// <param name="weekNo">Thứ tự tuần trong tháng</param>
        /// <returns>Ngày trả về kiểu DateTime</returns>
        /// Ex:
        ///     Thứ 2 tuần đầu tiên trong tháng tiếp theo của 2013/01/01 => 2013/02/04
        public static DateTime NextWeekdayOfMonth(this DateTime date, DayOfWeek weekday, WeekOfMonth weekNo)
        {
            DateTime targetDate = date.WeekdayOfMonth(weekday, weekNo);
            if (date < targetDate)
                return targetDate;
            else
                return date.AddMonths(1).WeekdayOfMonth(weekday, weekNo);
        }

        /// <summary>
        /// Thứ trong tháng
        /// </summary>
        /// <param name="date">Ngày cần tính</param>
        /// <param name="weekday">Thứ cần lấy</param>
        /// <param name="weekNo">Thứ tự tuần trong tháng</param>
        /// <returns>Ngày trả về kiểu DateTime</returns>
        /// Ex:
        ///     Thứ 2 tuần đầu tiên trong tháng của ngày 2013/01/01 => 2013/01/07
        public static DateTime WeekdayOfMonth(this DateTime date, DayOfWeek weekday, WeekOfMonth weekNo)
        {
            DateTime outputDate = date.AddDays(1 - date.Day);
            if (outputDate.DayOfWeek != weekday) outputDate = outputDate.NextWeekday(weekday);
            for (int i = 0; i < (int)weekNo; i++)
            {
                if (outputDate.NextWeekday(weekday).Month == date.Month)
                    outputDate = outputDate.NextWeekday(weekday);
                else
                    break;
            }
            return outputDate;
        }

        /// <summary>
        /// Thiết lập thời gian cho Ngày
        /// </summary>
        /// <param name="date">Ngày cần thiết lập thời gian</param>
        /// <param name="hour">Giờ được thiết lập</param>
        /// <param name="minute">Phút được thiết lập</param>
        /// <param name="second">Giây được thiết lập</param>
        /// <returns>Ngày trả về kiểu DateTime</returns>
        public static DateTime SetTimeToDate(this DateTime date, int hour, int minute, int second)
        {
            DateTime outputDate = new DateTime(date.Year, date.Month, date.Day, hour, minute, second);
            return DateTime.SpecifyKind(outputDate, DateTimeKind.Local);
        }
    }

    /// <summary>
    /// Thứ tự tuần trong tháng
    /// </summary>
    public enum WeekOfMonth
    {
        First = 0,
        Second = 1,
        Third = 2,
        Fourth = 3,
        Last = 4
    }
}
