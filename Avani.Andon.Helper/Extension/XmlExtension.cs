using System.Linq;

namespace Avani.Helper
{
    /// <summary>
    /// Cung cấp các method mở rộng cho Xml
    /// </summary>
    public static partial class XmlExtension
    {
        /// <summary>
        /// Set giá trị cho Attribute
        /// Nếu có thì cập nhật, chưa cho thì thêm mới
        /// </summary>
        /// <param name="el">XElement cần set</param>
        /// <param name="attrName">Tên thuộc tính</param>
        /// <param name="attrValue">Giá trị thuộc tính</param>
        public static void SetAttribute(this System.Xml.Linq.XElement el, string attrName, object attrValue)
        {
            if (el.Attribute(attrName) == null)
            {
                el.Add(new System.Xml.Linq.XAttribute(attrName, attrValue));
            }
            else
            {
                el.Attribute(attrName).SetValue(attrValue);
            }
        }
        /// <summary>
        /// Lấy giá trị Attribute
        /// </summary>
        /// <param name="el">XElement cần lấy thông tin</param>
        /// <param name="attr">Tên thuộc tính cần lấy</param>
        /// <returns>Giá trị thuộc tính trả về</returns>
        public static string GetAttribute(this System.Xml.Linq.XElement el, string attr)
        {
            return el.Attribute(attr) == null ? string.Empty : el.Attribute(attr).Value;
        }
        /// <summary>
        /// Lấy InnerXml của node
        /// </summary>
        /// <param name="el">XElement cần lấy thông tin</param>
        /// <returns>Trả về chuỗi là InnerXml của node</returns>
        public static string GetInnerXml(this System.Xml.Linq.XElement el)
        {
            string[] nodes = (from System.Xml.Linq.XElement n in el.Elements() select n.ToString()).ToArray();
            return string.Concat(nodes);
        }
    }
}
