using System;

namespace Avani.Helper
{
    /// <summary>
    /// Cung cấp các method mở rộng của Enum
    /// </summary>
    public static partial class EnumExtensions
    {
        /// <summary>
        /// Lấy mô tả của enum theo value
        /// </summary>
        /// <param name="enumValue">Enum value cần lấy mô tả</param>
        /// <returns>Trả về mô tả của enum value, nếu không có mô tả thì trả về tên của enum value</returns>
        public static string GetDescription(this Enum enumValue)
        {
            System.Reflection.FieldInfo fi = enumValue.GetType().GetField(enumValue.ToString());
            System.ComponentModel.DescriptionAttribute[] attributes = (System.ComponentModel.DescriptionAttribute[])fi.GetCustomAttributes(typeof(System.ComponentModel.DescriptionAttribute), false);
            if (attributes != null && attributes.Length > 0)
                return attributes[0].Description;
            else
                return enumValue.ToString();
        }
        /// <summary>
        /// Lấy enum value theo mô tả
        /// </summary>
        /// <typeparam name="T">Kiểu enum cần xử lý, Throw exception InvalidOperationException nếu không đúng kiểu enum</typeparam>
        /// <param name="description">Mô tả của enum value cần lấy</param>
        /// <returns>Trả về enum value có mô tả phù hợp, nếu không có thì trả về giá trị mặc định của enum</returns>
        public static T GetByDescription<T>(string description)
        {
            Type type = typeof(T);
            if (!type.IsEnum) throw new InvalidOperationException();
            T defaultValue = default(T);
            System.Reflection.FieldInfo[] fields = type.GetFields();
            foreach (System.Reflection.FieldInfo field in fields)
            {
                System.ComponentModel.DescriptionAttribute attribute = Attribute.GetCustomAttribute(field, typeof(System.ComponentModel.DescriptionAttribute)) as System.ComponentModel.DescriptionAttribute;
                if (attribute == null) continue;
                if (attribute.Description == description)
                {
                    return (T)field.GetValue(null);
                }
            }
            return defaultValue;
        }
    }
}
