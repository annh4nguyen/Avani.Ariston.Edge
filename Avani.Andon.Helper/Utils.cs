using System;
using System.Collections.Generic;
using System.Text;

namespace Avani.Helper
{
    /// <summary>
    /// Cung cấp các method tiện ích dùng chung
    /// </summary>
    public partial class Utils
    {
        /// <summary>
        /// Thực thi method từ 1 assembly khác
        /// </summary>
        /// <param name="assembly">Assembly fullname cần thực thi</param>
        /// <param name="classType">Class chứa method cần thực thi</param>
        /// <param name="methodName">Tên method cần thực thi</param>
        /// <param name="objectProps">Danh sách thuộc tính của class</param>
        /// <param name="methodParams">Danh sách tham số của method</param>
        /// <returns>Trả về object kết quả</returns>
        public static object Invoke(string assembly, string classType, string methodName, Dictionary<string, object> objectProps, object[] methodParams)
        {
            System.Runtime.Remoting.ObjectHandle handle = Activator.CreateInstance(assembly, classType);
            object obj = handle.Unwrap();
            Type type = obj.GetType();
            #region set properties
            if (objectProps != null)
            {
                foreach (KeyValuePair<string, object> pair in objectProps)
                {
                    System.Reflection.PropertyInfo prop = type.GetProperty(pair.Key);
                    if (prop != null) prop.SetValue(obj, pair.Value, null);
                }
            }
            #endregion
            #region invoke method
            System.Reflection.MethodInfo method = type.GetMethod(methodName);
            return method.Invoke(obj, methodParams);
            #endregion
        }
        /// <summary>
        /// Deserializes the specified XML.
        /// </summary>
        /// <typeparam name="TObject">The type of the object.</typeparam>
        /// <param name="xml">The XML.</param>
        /// <returns>Returns the TObject being deserialized.</returns>
        public static TObject Deserialize<TObject>(string xml)
        {
            TObject obj = default(TObject);

            try
            {
                if (!string.IsNullOrEmpty(xml))
                {
                    System.Xml.Serialization.XmlSerializer serializer = new System.Xml.Serialization.XmlSerializer(typeof(TObject));
                    using (System.IO.StringReader reader = new System.IO.StringReader(xml))
                    {
                        obj = (TObject)serializer.Deserialize(reader);
                    }
                }
            }
            catch { }

            return obj;
        }

        /// <summary>
        /// Serializes the specified obj.
        /// </summary>
        /// <typeparam name="TObject">The type of the object.</typeparam>
        /// <param name="obj">The obj.</param>
        /// <returns>Returns the XML document.</returns>
        public static string Serialize<TObject>(TObject obj)
        {
            string xml = string.Empty;

            try
            {
                if (obj != null)
                {
                    System.Xml.Serialization.XmlSerializer serializer = new System.Xml.Serialization.XmlSerializer(typeof(TObject));
                    using (StringWriterUtf8 writer = new StringWriterUtf8(System.Globalization.CultureInfo.InvariantCulture))
                    {
                        serializer.Serialize(writer, obj);
                        xml = writer.ToString();
                    }
                }
            }
            catch { }

            return xml;
        }
        /// <summary>
        /// Serializes the specified obj.
        /// </summary>
        /// <typeparam name="TObject">The type of the object.</typeparam>
        /// <param name="obj">The obj.</param>
        /// <param name="ns">The namespaces.</param>
        /// <returns>Returns the XML document.</returns>
        public static string Serialize<TObject>(TObject obj, System.Xml.Serialization.XmlSerializerNamespaces ns)
        {
            string xml = string.Empty;

            try
            {
                if (obj != null)
                {
                    System.Xml.Serialization.XmlSerializer serializer = new System.Xml.Serialization.XmlSerializer(typeof(TObject));
                    using (StringWriterUtf8 writer = new StringWriterUtf8(System.Globalization.CultureInfo.InvariantCulture))
                    {
                        serializer.Serialize(writer, obj, ns);
                        xml = writer.ToString();
                    }
                }
            }
            catch { }

            return xml;
        }
    }
    public class StringWriterUtf8 : System.IO.StringWriter
    {
        public StringWriterUtf8(IFormatProvider formatProvider) : base(formatProvider)
        {
        }
        public override Encoding Encoding
        {
            get
            {
                return Encoding.UTF8;
            }
        }
    }
}
