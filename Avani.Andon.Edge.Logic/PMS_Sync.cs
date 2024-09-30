using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Collections;
using Avani.Helper;
using System.Configuration;
using EasyNetQ;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Net.Http.Headers;
using System.Net.Http;
using EasyNetQ.SystemMessages;
using System.Collections.Generic;
using System.Reflection.Emit;
using EasyNetQ.Interception;
using iAndon.MSG;
using System.Data.SqlClient;
using System.Xml.Linq;
using System.Numerics;
//using Avani.Andon.Resources;

namespace Avani.Andon.Edge.Logic
{
    public class PMS_Sync
    {
        private Log _Logger = Avani.Andon.Edge.Logic.Helper.GetLog();
        private Log _Rawer = Avani.Andon.Edge.Logic.Helper.GetRaws();

        private readonly string _LogCategory = "PMS_Sync";
        private string _LogPath = ConfigurationManager.AppSettings["log_path"];

        public static string _Sync_Url;
        public static int _SyncInterval;
        public string _Sync_Code;

        /// <summary>
        /// Local Variables Declaration.
        /// </summary>
        private string _RabbitMQHost = ConfigurationManager.AppSettings["RabbitMQ.Host"];
        private string _RabbitMQVirtualHost = ConfigurationManager.AppSettings["RabbitMQ.VirtualHost"];
        private string _RabbitMQUser = ConfigurationManager.AppSettings["RabbitMQ.User"];
        private string _RabbitMQPassword = ConfigurationManager.AppSettings["RabbitMQ.Password"];
        private string _CustomerID = ConfigurationManager.AppSettings["CustomerID"];
        private int _TimeToGetProduction = Convert.ToInt32(ConfigurationManager.AppSettings["last_time_to_get_production"]);
        

        private List<string> LineCodes = new List<string>();
        
        private System.Timers.Timer _TimerProccessSync = new System.Timers.Timer();

        private IBus _EventBus;
        /// <summary>
        /// Constructors.
        /// </summary>
        public PMS_Sync(string _url, int _interval, string _codes)
        {
            try
            {
                _Sync_Url = _url;
                _SyncInterval = _interval;
                _Sync_Code = _codes;
   
            }
            catch(Exception ex)
            {
                _Logger.Write(_LogCategory, $"Init Sync Error: {ex}", LogType.Error);
            }
        }
        /// <summary>
        /// Destructor.
        /// </summary>
        ~PMS_Sync()
        {
            try
            {
                Stop();
            }
            catch(Exception ex)
            {
                _Logger.Write(_LogCategory, ex);
            }
        }

        /// <summary>
        /// Init method that create a server (TCP Listener) Object based on the
        /// IP Address and Port information that is passed in.
        /// </summary>
        /// <param name="endPoint"></param>
        private void Init()
        {
            try
            {
                string[] arr = _Sync_Code.Split(';');
                foreach (string code in arr)
                {
                    LineCodes.Add(code);
                }
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Init Data for Sync Error: {ex}", LogType.Error);
            }
        }

        /// <summary>
        /// Method that starts TCP/IP Server.
        /// </summary>
        public void Start()
        {
            try
            {
                Init();

                _Logger.Write(_LogCategory, "Start Sync PMS", LogType.Info);
                _TimerProccessSync.Interval = _SyncInterval;
                _TimerProccessSync.Elapsed += _TimerProccessSync_Elapsed;
                _TimerProccessSync.Start();
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Start Sync Error: {ex}", LogType.Error);
            }
        }

        /// <summary>
        /// Method that stops the TCP/IP Server.
        /// </summary>
        public void Stop()
        {
            try
            {
                LineCodes.Clear();
                _TimerProccessSync.Stop();
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Stop Sync Error: {ex}", LogType.Error);
            }
        }

        private void _TimerProccessSync_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            System.Timers.Timer timer = sender as System.Timers.Timer;
            timer.Stop();
            try
            {
                ProccessSync();
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Proccess Sync Error: {ex}", LogType.Error);
            }
            finally
            {
                timer.Start();
                //_TimerProccessWork.Start();
            }
        }
        private void ProccessSync()
        {
            try
            {
                DateTime eventTime = DateTime.Now;

                foreach (string code in LineCodes)
                {
                    try
                    {
                        PMS_BodyMessage result = GetPMSInfo(code);

                        if (result != null)
                        {
                            //Kiểm tra, nếu quá cũ thì bỏ qua
                            DateTime lastProductTime = DateTime.Parse(result.lastproductiontime);
                            double _duration = (eventTime - lastProductTime).TotalSeconds;
                            if (_duration > _TimeToGetProduction) continue;

                            //Gửi lên Rabbit
                            try
                            {
                                PMS_MSG message = new PMS_MSG("PMS", DateTime.Now, MessageType.PMS, result);
                                if (_EventBus == null)
                                {
                                    // try connect to rabbitmq
                                    ConnectRabbitMQ();
                                }

                                if (!_EventBus.IsConnected)
                                {
                                    // try connect to rabbitmq
                                    ConnectRabbitMQ();
                                }

                                string _rawMessage = JsonConvert.SerializeObject(result);

                                if (_EventBus != null && _EventBus.IsConnected)
                                {
                                    _EventBus.Publish<iAndon.MSG.PMS_MSG>(message);
                                    WriteRawsData(_rawMessage, code);
                                }
                                else
                                {
                                    _Logger.Write(_LogCategory, $" [{_rawMessage}]", LogType.Error, "_Error_" + code);
                                }


                            }
                            catch (Exception ex)
                            {
                                _Logger.Write(_LogCategory, $"Send to RabbitMQ Sync Line [{code}] Error: {ex}", LogType.Error);
                            }
                        }
                    }
                    catch (Exception ex1)
                    {
                        _Logger.Write(_LogCategory, $"Proccess Sync Line [{code}] Error: {ex1}", LogType.Error);
                    }
                }

            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Proccess Sync Error: {ex}", LogType.Error);
            }
        }

        private void ConnectRabbitMQ()
        {
            try
            {
                _EventBus = RabbitHutch.CreateBus($"host={_RabbitMQHost};virtualHost={_RabbitMQVirtualHost};username={_RabbitMQUser};password={_RabbitMQPassword}");
                if (_EventBus != null && _EventBus.IsConnected)
                {
                    _Logger.Write(_LogCategory, $"PMS client connected to RabbitMQ!", LogType.Info);
                }

            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, ex);
            }
        }
        /// <summary>
        /// Method that stops all clients and clears the list.
        /// </summary>
        private PMS_BodyMessage GetPMSInfo(string CODE)
        {
            PMS_BodyMessage result = null;
            try
            {
                //Lấy PMS thực tế

                HttpClient client = new HttpClient();
                client.BaseAddress = new Uri(_Sync_Url);

                // Add an Accept header for JSON format.
                client.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("application/json"));

                // List data response.
                HttpResponseMessage response = client.GetAsync(CODE).Result;
                if (response.IsSuccessStatusCode)
                {
                    // Parse the response body.
                    string responseString = response.Content.ReadAsStringAsync().Result;
                    JObject jsonObj = JObject.Parse(responseString);
                    string content = jsonObj["content"].ToString();
                    //_Logger.Write(_LogCategory, $"PMS call for LINe {CODE}: {content}", LogType.Debug);
                    result = JsonConvert.DeserializeObject <PMS_BodyMessage> (content);
                }
                else
                {
                    _Logger.Write(_LogCategory, $"PMS call for LINe {CODE} NOT SUCCESSFULL", LogType.Error);
                }
                //Dispose once all HttpClient calls are complete. This is not necessary if the containing object will be disposed of; for example in this case the HttpClient instance will be disposed automatically when the application terminates so the following call is superfluous.
                client.Dispose();

                //Lấy PMS giả để test
/*
                string _conn = "data source=27.72.56.75,7023;initial catalog=AVANI_ARISTON;persist security info=True;user id=fts_avani;password=FTSvn@AVANI;MultipleActiveResultSets=True;";
                using (SqlConnection myConnection = new SqlConnection(_conn))
                {
                    string oString = $"Select * from MES_TMP_PMS_DATA where ProductLineId='{CODE}' ORDER BY [LastProductionTime] DESC";
                    SqlCommand oCmd = new SqlCommand(oString, myConnection);
                    myConnection.Open();
                    using (SqlDataReader oReader = oCmd.ExecuteReader())
                    {
                        while (oReader.Read())
                        {
                            result = new PMS_BodyMessage()
                            {
                                productlineid = int.Parse(oReader["ProductLineId"].ToString()),
                                productcode = oReader["ProductCode"].ToString(),
                                productname = oReader["ProductName"].ToString(),
                                planid = double.Parse(oReader["PlanId"].ToString()),
                                ponumber = double.Parse(oReader["PONumber"].ToString()),
                                model = oReader["Model"].ToString(),
                                planquantity = int.Parse(oReader["PlanQuantity"].ToString()),
                                actualquantity = int.Parse(oReader["ActualQuantity"].ToString()),
                                lastproductiontime = oReader["LastProductionTime"].ToString(),
                                status = oReader["Status"].ToString(),
                            };

                            break;
                        }
                        myConnection.Close();
                    }
                }
*/
                return result;

            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Proccess Sync Call PMS line {CODE} Error: {ex}", LogType.Error);
            }
            return null;
        }

        private void WriteRawsData(string _rawMessage, string CODE)
        {
            _Rawer.Write(_LogCategory, $"{_rawMessage}", LogType.Info, "PMS_" + CODE);
        }


    }
}
