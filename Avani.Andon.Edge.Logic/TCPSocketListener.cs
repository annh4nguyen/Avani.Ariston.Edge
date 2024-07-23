using System;
using System.Net.Sockets;
using System.Threading;
using System.Text;
using System.IO;
using EasyNetQ;
using System.Configuration;
using Avani.Helper;
using System.Reflection;
using System.Net;
using System.Net.WebSockets;
using System.Data.SqlClient;
using System.IO.Ports;
using System.CodeDom;
using System.Collections.Generic;
using System.Linq;
using System.Net.Configuration;
using Newtonsoft.Json;
using iAndon.MSG;

namespace Avani.Andon.Edge.Logic
{
    /// <summary>
    /// Summary description for TCPSocketListener.
    /// </summary>
    public class TCPSocketListener
    {
        private Log _Logger;
        private readonly string _LogCategory = "TCPSocketListener";

        private string _RabbitMQHost = ConfigurationManager.AppSettings["RabbitMQ.Host"];
        private string _RabbitMQVirtualHost = ConfigurationManager.AppSettings["RabbitMQ.VirtualHost"];
        private string _RabbitMQUser = ConfigurationManager.AppSettings["RabbitMQ.User"];
        private string _RabbitMQPassword = ConfigurationManager.AppSettings["RabbitMQ.Password"];
        private string _CustomerID = ConfigurationManager.AppSettings["CustomerID"];

        private string _LogPath = ConfigurationManager.AppSettings["log_path"];


        private int _Disconnect_Interval = 1000 * int.Parse(ConfigurationManager.AppSettings["disconnect_interval"]);
        private int _Error_Interval = int.Parse(ConfigurationManager.AppSettings["error_interval"]);
        private int _PingInterval = int.Parse(ConfigurationManager.AppSettings["request_interval"]); //Tính = milisecond luôn
        private string _PingMessage = ConfigurationManager.AppSettings["ping_message"];
        private bool _IsPingInterval = (ConfigurationManager.AppSettings["ping_client"] == "1");
        private int _MessageLength = int.Parse(ConfigurationManager.AppSettings["message_length"]); 

        private static long MAX_INT32 = 32767;
        private static long MAX_INT64 = 65535;

        //private Messages.AndonMessage _Message = new Messages.AndonMessage();
        /// <summary>
        /// Variables that are accessed by other classes indirectly.
        /// </summary>
        private TcpClient _TcpClient = null;
        private string _ClientIP = "";
        private string _ClientDeviceId = "";
        private bool _StopClient = false;
        private Thread _ListenerThread = null;
        private Thread _PingThread = null;
        private bool _MarkedForDeletion = false;
        private IBus _EventBus;

        string _CacheMessage = "";


        /// <summary>
        /// Working Variables.
        /// </summary>
        private StringBuilder _OneLine = new StringBuilder();

        private DateTime _LastReceived;
        //private DateTime _LastPing;
        private DateTime _CurrentReceived;

        /// <summary>
        /// Client Socket Listener Constructor.
        /// </summary>
        /// <param name="tcpClient"></param>
        public TCPSocketListener(TcpClient tcpClient)
        {
            try
            {
                _Logger = Avani.Andon.Edge.Logic.Helper.GetLog();
                _TcpClient = tcpClient;
                string clientEndpoint = $"{_TcpClient.Client.RemoteEndPoint}";
                string clientIP = ((IPEndPoint)_TcpClient.Client.RemoteEndPoint).Address.ToString();
                _ClientIP = clientEndpoint;

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Client SocketListener Destructor.
        /// </summary>
        ~TCPSocketListener()
        {
            try
            {
                StopSocketListener();
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, ex);
            }
        }

        public void StartSocketListener()
        {
            try
            {
                if (_TcpClient != null)
                {
                    _ListenerThread = new Thread(new ThreadStart(SocketListenerThreadStart));
                    _ListenerThread.Start();

                    _PingThread = new Thread(new ThreadStart(PingClientInterval));
                    _PingThread.Start();

                    //StartGetMessage();
                }
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, ex);
            }
        }

        private void SocketListenerThreadStart()
        {
            try
            {
                string clientEndpoint = $"{_TcpClient.Client.RemoteEndPoint}";
                string clientIP = ((IPEndPoint)_TcpClient.Client.RemoteEndPoint).Address.ToString();
                _ClientIP = clientEndpoint;
                _Logger.Write(_LogCategory, $"Client {clientEndpoint} Connected!", LogType.Debug);

                NetworkStream stream = _TcpClient.GetStream();
                Byte[] byteBuffer = new Byte[1024];
                int size;

                _LastReceived = DateTime.Now;
                _CurrentReceived = _LastReceived;
                //_LastPing = DateTime.Now;

                Timer t_checkDisconnected = new Timer(new TimerCallback(CheckClientCommInterval), clientIP, _Disconnect_Interval, _Disconnect_Interval);
                try
                {
                    while (!_StopClient)
                    {
                        while ((size = stream.Read(byteBuffer, 0, byteBuffer.Length)) != 0)
                        {
                            //_Logger.Write(_LogCategory, $"Received from {_ClientIP}", LogType.Debug);
                            _CurrentReceived = DateTime.Now;
                            ParseReceiveBuffer(clientIP, _CurrentReceived, byteBuffer, size);
                        }
                        Thread.Sleep(300);
                        _Logger.Write(_LogCategory, $"Client {_ClientIP} try connecting...", LogType.Debug);
                        DateTime _currentTime = DateTime.Now;
                        if ((_currentTime - _CurrentReceived).TotalMilliseconds > 1000)
                        {
                            break;
                        }
                    }
                    _Logger.Write(_LogCategory, $"Client {_ClientIP} Disconnect", LogType.Debug);
                    _StopClient = true;
                    _MarkedForDeletion = true;
                }
                catch (Exception ex)
                {
                    _Logger.Write(_LogCategory, $"Client {_ClientIP} Listener Error: {ex.Message}", LogType.Error);
                    _StopClient = true;
                    _MarkedForDeletion = true;
                }
                t_checkDisconnected.Change(Timeout.Infinite, Timeout.Infinite);
                t_checkDisconnected = null;
            }
            catch (Exception e1)
            {
                _Logger.Write(_LogCategory, e1);
            }
        }




        /// <summary>
        /// Method that stops Client SocketListening Thread.
        /// </summary>
        public void StopSocketListener()
        {
            _Logger.Write(_LogCategory, $"Stop Client {_ClientIP}", LogType.Info);
            try
            {
                if (_TcpClient != null)
                {
                    _StopClient = true;
                    _TcpClient.Close();
                    if (_EventBus != null)
                    {
                        _EventBus.Dispose();
                    }
                    // Wait for one second for the the thread to stop.
                    _ListenerThread.Join(1000);

                    // If still alive; Get rid of the thread.
                    if (_ListenerThread.IsAlive)
                    {
                        _ListenerThread.Abort();
                    }
                    _ListenerThread = null;

                    /*
                    _PingThread.Join(1000);
                    if(_PingThread.IsAlive)
                    {
                        _PingThread.Abort();
                    }
                    _PingThread = null;
                    */

                    _TcpClient = null;
                    _MarkedForDeletion = true;
                }

            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Stop Client Error: {ex}", LogType.Error);
            }
        }

        public bool IsMarkedForDeletion()
        {
            return _MarkedForDeletion;
        }

        public void SendCommand(string message)
        {
            try
            {
                if (_StopClient) return;
                    byte[] data = Encoding.ASCII.GetBytes(message);

                    _TcpClient.Client.Send(data);
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"SendCommand Error: {ex}", LogType.Error);
            }
        }

        public string ByteArrayToString(byte[] ba, int size)
        {
            //StringBuilder hex = new StringBuilder(ba.Length * 2);
            StringBuilder hex = new StringBuilder(size * 2);

            foreach (byte b in ba)
                hex.AppendFormat("{0:x2}", b);
            return hex.ToString();
        }
    
        private void CheckClientCommInterval(object clientIP)
        {
            try
            {
                if (!_StopClient)
                {
                    _Logger.Write(_LogCategory, $"Check Client {_ClientIP}, Last Received: {_LastReceived:HH:mm:ss.FFF}, Current Received: {_CurrentReceived:HH:mm:ss.FFF}", LogType.Debug);
                    if (_LastReceived.Equals(_CurrentReceived))
                    {
                        _StopClient = true;
                        _MarkedForDeletion = true;
                    }
                    else
                    {
                        _LastReceived = _CurrentReceived;
                    }
                }
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, ex);
            }
        }

        private void PingClientInterval()
        {
            if (!_IsPingInterval)
                return;
            try
            {
                while (!_StopClient)
                {
                    try
                    {

                        byte[] data = Encoding.ASCII.GetBytes(_PingMessage);

                        _Logger.Write(_LogCategory, $"Ping Client {_ClientIP} ", LogType.Debug);

                        _TcpClient.Client.Send(data);

                    }
                    catch (Exception ex)
                    {
                        _Logger.Write(_LogCategory, $"Ping {_ClientIP} Error: {ex}", LogType.Error);
                    }
                    Thread.Sleep(_PingInterval);
                }
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, ex);
            }
        }


        /// <summary>
        /// Bắt đầu nhận message từ Queue
        /// </summary>
        public void StartGetMessage()
        {
            return;
   
        }

 
    

        #region RabbitMQ
        /// <summary>
        /// Connect to RabbitMQ
        /// </summary>
        private void ConnectRabbitMQ()
        {
            try
            {
                //IBus __EventBus = RabbitHutch.CreateBus($"host=27.72.29.38:5672;virtualHost=/;username=genset;password=genset");
                _EventBus = RabbitHutch.CreateBus($"host={_RabbitMQHost};virtualHost={_RabbitMQVirtualHost};username={_RabbitMQUser};password={_RabbitMQPassword}");
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Connect to RabbitMQ Error: {ex}", LogType.Error);
            }
        }

        #endregion

        #region ProcessData
        private void ParseReceiveBuffer(string clientIP, DateTime receivedTime, Byte[] content, int size)
        {
            try
            {
                string data = Encoding.ASCII.GetString(content, 0, size);

                //// store raw data
                //StoreRawData(clientIP, receivedTime, data);

                //AnNH edit
                string[] separatingStrings = { "\r\n"};
                string[] arrayData = data.Split(separatingStrings, StringSplitOptions.RemoveEmptyEntries);

                foreach(string _line in arrayData)
                {
                    if (_line.StartsWith(":D"))
                    {
                       //Messages.Body _body = new AndonMessage().StringToMessage(_line, 0, _MessageLength, 0);
                       // if (_body != null)
                       // {
                       //     ProcessClientData(clientIP, receivedTime, _body);
                       // }
                    }
                }


                //CongNC code
                //int lineEnd = 0;
                //do
                //{
                //    lineEnd = data.IndexOf("\r\n");
                //    if (lineEnd != -1)
                //    {
                //        _OneLine = _OneLine.Append(data, 0, lineEnd + 2);
                //        ProcessClientData(clientIP, receivedTime, _OneLine.ToString());
                //        _OneLine.Remove(0, _OneLine.Length);
                //        data = data.Substring(lineEnd + 2, data.Length - lineEnd - 2);
                //    }
                //    else
                //    {
                //        _OneLine = _OneLine.Append(data);
                //    }
                //} while (lineEnd != -1);
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, ex);
            }
        }

        /*
        private void ProcessClientData(string clientIP, DateTime receivedTime, Messages.Body _message)
        {
            try
            {
                // publish
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

                string strMessage = JsonConvert.SerializeObject(_message);
                _Logger.Write(_LogCategory, $"Data from {_ClientIP}: {strMessage} ", LogType.Debug);

                //Lưu message
                new AndonMessage().StoreRawData(_Logger, _LogPath, _LogCategory, clientIP, receivedTime, strMessage);

                if (_EventBus != null && _EventBus.IsConnected)
                {
                    Messages.AndonMessage message = new Messages.AndonMessage(clientIP, receivedTime, Messages.AndonMessageType.Andon, _message);
                    _EventBus.Publish<Messages.AndonMessage>(message);
                }
                else
                {
                    new AndonMessage().StoreErrorMessage(_Logger, _LogPath, _LogCategory, clientIP, receivedTime, strMessage, _Error_Interval);
                }
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, ex);
            }
        }
        */

        #endregion

        #region ParserMessage

        private void ParseMessage(string data)
        {
            throw new NotImplementedException();
        }
     
        #endregion


    }
}
