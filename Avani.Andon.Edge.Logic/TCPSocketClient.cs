using EasyNetQ;
using SuperSocket.ClientEngine;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Avani.Helper;
using Newtonsoft.Json;
using iAndon.MSG;
using System.Timers;

public delegate void ReceiveIncomingDataEventRaise(string message);

namespace Avani.Andon.Edge.Logic
{
    public class TCPSocketClient : AsyncTcpSession
    {

        private Log _Logger = Avani.Andon.Edge.Logic.Helper.GetLog();
        private Log _Rawer = Avani.Andon.Edge.Logic.Helper.GetRaws();

        private readonly string _LogCategory = "TCPSocketClient";
        private string _LogPath = ConfigurationManager.AppSettings["log_path"];

        public event ReceiveIncomingDataEventRaise OnReceivedMessage;

        private string _RabbitMQHost = ConfigurationManager.AppSettings["RabbitMQ.Host"];
        private string _RabbitMQVirtualHost = ConfigurationManager.AppSettings["RabbitMQ.VirtualHost"];
        private string _RabbitMQUser = ConfigurationManager.AppSettings["RabbitMQ.User"];
        private string _RabbitMQPassword = ConfigurationManager.AppSettings["RabbitMQ.Password"];
        private int _ReconnectInterval = 60 * 1000 * int.Parse(ConfigurationManager.AppSettings["reconnect_interval"]); //Tính = phút

        private int _Disconnect_Interval = 1000 * int.Parse(ConfigurationManager.AppSettings["disconnect_interval"]);
        private int _Error_Interval = int.Parse(ConfigurationManager.AppSettings["error_interval"]);
        private int _RequestInterval = 1000 * int.Parse(ConfigurationManager.AppSettings["request_interval"]); //Tính = second
        private int _SendInterval = 1000 * int.Parse(ConfigurationManager.AppSettings["send_interval"]); //Tính = second


        private int _PingInterval = 1000*int.Parse(ConfigurationManager.AppSettings["ping_interval"]); //Tính = second
        private bool _IsPingInterval = (ConfigurationManager.AppSettings["ping_client"] == "1");
        private string _PingMessage = ConfigurationManager.AppSettings["ping_message"];
        private int _MessageLength = int.Parse(ConfigurationManager.AppSettings["message_length"]);
        private int _DeviceNumberOnGateway = int.Parse(ConfigurationManager.AppSettings["DeviceNumberOnGateway"]);

        private Dictionary<int, Andon_MSG> Nodes = new Dictionary<int, Andon_MSG>();

        //public static int TIMER_SCHEDULE_CHECK_DISCONNECT = Convert.ToInt32(ConfigurationManager.AppSettings["TIMER_SCHEDULE_CHECK_DISCONNECT"].Trim());//mili giay
        //public static int TIME_CONFIRM_DISCONNECT = Convert.ToInt32(ConfigurationManager.AppSettings["TIME_CONFIRM_DISCONNECT"].Trim());//phut
        public static int TIME_WAIT_CONNECT = Convert.ToInt32(ConfigurationManager.AppSettings["TIME_WAIT_CONNECT"].Trim());//mili giay
        public static int TIME_SLEEP_SEND = Convert.ToInt32(ConfigurationManager.AppSettings["TIME_SLEEP_SEND"].Trim());//mili giay
        public static int TIME_NOT_RECEIVE_DATA = Convert.ToInt32(ConfigurationManager.AppSettings["TIME_NOT_RECEIVE_DATA"].Trim());//giay
        public static int SEND_RESET_NOT_RESPONSE = Convert.ToInt32(ConfigurationManager.AppSettings["SEND_RESET_NOT_RESPONSE"].Trim());//giay
        public static int RESET_AFTER_NOT_RECEIVE_DATA = 1000 * Convert.ToInt32(ConfigurationManager.AppSettings["RESET_AFTER_NOT_RECEIVE_DATA"].Trim());//Time

        private bool _IsInvertInput = (ConfigurationManager.AppSettings["INVERT_INPUT"] == "1");

        public static byte NumberOfRegister = 6;


        private System.Timers.Timer _TimerPingCommand = new System.Timers.Timer();
        private System.Timers.Timer _TimerSendCommand = new System.Timers.Timer();
        //private System.Timers.Timer _TimerReconnect = new System.Timers.Timer();
        private System.Timers.Timer _TimerCheckNoData = new System.Timers.Timer();

        public int Id { get; set; }
        public string ServerIP { get; set; }
        public int NumberOfNodes { get; set; }
        public int ServerPort { get; set; }
        public int CountForNoData { get; set; } //Dùng để đếm xem sau bao nhiêu lần ko nhận được dữ liệu thì reset ngay

        private IBus _EventBus;

        IPEndPoint remoteEndpoint;

        private string strAvCache = "";
        public DateTime LastTimeReceiveData = DateTime.Now;

        bool isRunning = false;
        readonly object lockReceiveData = new object();
        readonly object lockConnectSync = new object();

        public TCPSocketClient(int _id, string _ip, int _port, int _numberOfNodes = 0)
        {
            try
            {
                Id = _id;
                ServerIP = _ip;
                ServerPort = _port;
                NumberOfNodes = _numberOfNodes;

                //Setup Nodes
                for (int i = 1; i <= _numberOfNodes; i++)
                {
                    Nodes.Add(i, new Andon_MSG(ServerIP, LastTimeReceiveData, MessageType.Andon, i.ToString(), 0, 0, 0, 0, 0, 0));
                }
                //rabbit = _rabbit;
                Setup();
                //ConnectRabbitMQ();
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Error when int Gateway {_ip} - Port {_port} - Nodes {_numberOfNodes}: {ex.Message}", LogType.Error);
            }

        }

        void Setup()
        {
            isRunning = true;
            remoteEndpoint = new IPEndPoint(IPAddress.Parse(ServerIP), ServerPort);

            Connected += SessionConnected;
            Closed += SessionClosed;
            DataReceived += SessionOnDataReceived;
            Error += SessionError;
            CountForNoData = 0;
            //_Logger = Avani.Andon.Edge.Logic.Helper.GetLog();

            //Send PING Command
            _TimerPingCommand.Interval = _RequestInterval;
            _TimerPingCommand.Elapsed += _TimerProccessPingCommand_Elapsed;
            _TimerPingCommand.Start();


            //Send Control Command
            _TimerSendCommand.Interval = _SendInterval;
            _TimerSendCommand.Elapsed += _TimerProccessSendCommand_Elapsed;
            _TimerSendCommand.Start();


            //Reconnect gateway 
            //_TimerReconnect.Interval = _ReconnectInterval;
            //_TimerReconnect.Elapsed += _TimerReconnect_Elapsed;
            //_TimerReconnect.Start();

            //Check NoData gateway 
            _TimerCheckNoData.Interval = _ReconnectInterval;
            _TimerCheckNoData.Elapsed += _TimerCheckNoData_Elapsed;
            _TimerCheckNoData.Start();

        }

        private void _TimerProccessPingCommand_Elapsed(object sender, ElapsedEventArgs e)
        {
            try
            {
                _TimerPingCommand.Stop();
                if (_IsPingInterval)
                {
                    //byte[] bytes = Encoding.ASCII.GetBytes(_PingMessage + "\r\n");
                    //this.Client.Send(bytes);
                    //Thread.Sleep(TIME_SLEEP_SEND);

                    ushort _startAddress = 0, _numberOfRegisters = 4;
                    byte _functionCode = 2;
                    string _msg = "";
                    for (byte _node = 1; _node <= NumberOfNodes; _node++)
                    {
                        //Gửi lệnh request 

                        //Bộ ZIGBEE
                        //_msg = new AndonMessage().PackageRequest(_node);
                        //byte[] data = Encoding.ASCII.GetBytes(_msg);

                        //Bộ LonHand
                        //_msg = ModbusRTUOverTCP.ReadDiscreteInputsMsg(_node, _startAddress, _functionCode, _numberOfRegisters);
                        byte[] data = ModbusRTUOverTCP.ReadDiscreteInputsMsg(_node, _startAddress, _functionCode, _numberOfRegisters);
                        _msg = ByteArrayToString(data);
                        if (this.IsConnected)
                        {
                            //_Logger.Write(_LogCategory, $"Ping Client {this.ServerIP} - Slave {_node}: {_msg}", LogType.Debug);
                            this.Client.Send(data);
                            Thread.Sleep(TIME_SLEEP_SEND);
                        }
                    }


                }
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Error when send command at {ServerIP} to node 31!", LogType.Debug);
            }
            finally
            {
                _TimerPingCommand.Start();
            }
        }


        private void _TimerProccessSendCommand_Elapsed(object sender, ElapsedEventArgs e)
        {
            try
            {
                _TimerSendCommand.Stop();
                DateTime eventTime = DateTime.Now;
                byte _in1 = 0, _in2 = 0, _in3 = 0, _in4 = 0, _in5 = 0, _in6 = 0;
                //string _msg = "";
                //Sau khi nhận được, gửi đi ==> Xử lý thằng đầu chuyền.
                for (int _id = 1; _id <= NumberOfNodes; _id++)
                {
                    Andon_MSG nodeMsg = Nodes[_id];
                    if (nodeMsg.Body.In01 == 1) _in1 = 1;
                    if (nodeMsg.Body.In02 == 1) _in2 = 1;
                    //if (nodeMsg.Body.In03 == 1) _in3 = 1;
                    //if (nodeMsg.Body.In04 == 1) _in4 = 1;
                    //if (nodeMsg.Body.In05 == 1) _in5 = 1;
                    //if (nodeMsg.Body.In06 == 1) _in6 = 1;

                    //Send to Node for Light
                    int _nodeValue = 1 * nodeMsg.Body.In01;
                    if (nodeMsg.Body.In02 == 1) { _nodeValue = 2; }
                    //int _nodeValue = 1 * nodeMsg.Body.In01 + 2 * nodeMsg.Body.In02 + 4 * nodeMsg.Body.In03 + 8 * nodeMsg.Body.In04;

                    byte[] dataNode = ModbusRTUOverTCP.WriteMultiCoilsMsg((byte)_id, 0, 2, 15, (byte)_nodeValue);
                    string _msgNode = ByteArrayToString(dataNode);

                    if (this.IsConnected)
                    {
                        //_Logger.Write(_LogCategory, $"Starting send command at {ServerIP} to Local node {_id}: {_msgNode}!", LogType.Debug);
                        this.Client.Send(dataNode);
                        Thread.Sleep(TIME_SLEEP_SEND);
                        //_Logger.Write(_LogCategory, $"Finished send command at {ServerIP} to Local node {_id}!", LogType.Debug);
                    }
                }

                //Send to EndOfLine Node

                int _lineValue = _in1;
                if (_in2 == 1) { _lineValue = 2; }

                byte[] dataLine = ModbusRTUOverTCP.WriteMultiCoilsMsg(31, 0, 2, 15, (byte)_lineValue);
                string _msgLine = ByteArrayToString(dataLine);

                if (this.IsConnected)
                {
                    //_Logger.Write(_LogCategory, $"Starting send command at {ServerIP} to node 31: {_msgLine}!", LogType.Debug);
                    this.Client.Send(dataLine);
                    Thread.Sleep(TIME_SLEEP_SEND);
                    //_Logger.Write(_LogCategory, $"Finished send command at {ServerIP} to node 31!", LogType.Debug);
                }


            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Error when send command at {ServerIP} to node 31!", LogType.Debug);
            }
            finally
            {
                _TimerSendCommand.Start();

            }
        }

        //private void _TimerReconnect_Elapsed(object sender, ElapsedEventArgs e)
        //{
        //    _TimerReconnect.Stop();

        //    ReConnect();

        //    _TimerReconnect.Start();
        //}

        
        private void _TimerCheckNoData_Elapsed(object sender, ElapsedEventArgs e)
        {
            _TimerCheckNoData.Stop();
            if (!this.IsConnected)
            {
                Connect();
            }
            if (this.IsConnected)
            {
                double _not_receive_duration = (DateTime.Now - LastTimeReceiveData).TotalSeconds;
                if (_not_receive_duration > RESET_AFTER_NOT_RECEIVE_DATA)
                {
                    ReConnect();
                }
            }

            _TimerCheckNoData.Start();
        }

        private void ConnectRabbitMQ()
        {
            try
            {
                _EventBus = RabbitHutch.CreateBus($"host={_RabbitMQHost};virtualHost={_RabbitMQVirtualHost};username={_RabbitMQUser};password={_RabbitMQPassword}");
                if (_EventBus != null && _EventBus.IsConnected)
                {
                    _Logger.Write(_LogCategory, $"Client {this.ServerIP} connected to RabbitMQ!", LogType.Info);
                }

            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, ex);
            }
        }
        //public bool SendDataToDevice(string message)
        //{
        //    bool isSendSuccess = false;
        //    try
        //    {
        //        if (!this.IsConnected)
        //        {
        //            this.Connect(remoteEndpoint);
        //            Thread.Sleep(100);
        //        }

        //        if (this.IsConnected)
        //        {
        //            byte[] messageByte = Encoding.ASCII.GetBytes(message);
        //            this.Send(messageByte, 0, message.Length);
        //            isSendSuccess = true;
        //        }
        //        else
        //        {
        //            ReConnect();
        //            byte[] messageByte = Encoding.ASCII.GetBytes(message);
        //            this.Send(messageByte, 0, message.Length);

        //            //log.Info("TCPSocket send data: " + message);
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        isSendSuccess = false;
        //    }

        //    return isSendSuccess;
        //    //try
        //    //{
        //    //    this.Close();
        //    //}
        //    //catch { }

        //}

        void Connect()
        {
            try
            {
                lock (lockConnectSync)
                {
                    if (!this.IsConnected)
                    {
                        this.Connect(remoteEndpoint);
                    }
                    Thread.Sleep(TIME_WAIT_CONNECT);
                    if (this.IsConnected)
                    {
                        _TimerPingCommand.Start();
                        _TimerSendCommand.Start();

                        //Khởi tạo nó tính từ đó là nhận dữ liệu
                        LastTimeReceiveData = DateTime.Now;
                    }
                }
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Error when Connect to {ServerIP}:{ex}", LogType.Error);
            }

        }
        void ReConnect()
        {
            _Logger.Write(_LogCategory, $"Reconnecting to gateway {ServerIP}...!", LogType.Debug);

            lock (lockConnectSync)
            {
                try
                {
                    Close();
                    Thread.Sleep(TIME_WAIT_CONNECT);
                }
                catch { }
                try
                {
                    Connect();
                }
                catch(Exception ex) {
                    _Logger.Write(_LogCategory, $"Error when ReConnect to {ServerIP}:{ex}", LogType.Error);
                }
            }

            //CountForNoData++;

        }

        public void OpenConnectLoop()
        {
            try
            {
                Task.Factory.StartNew(() =>
                {
                    while (isRunning)
                    {
                        if (!this.IsConnected)
                        {
                            //_Logger.Write(_LogCategory, $"Connecting to gateway {ServerIP}...!", LogType.Debug);
                            Connect();

                        }
                        Thread.Sleep(TIME_WAIT_CONNECT);

                        double timeDurationReceiveData = (DateTime.Now - LastTimeReceiveData).TotalSeconds;
                        if (timeDurationReceiveData > SEND_RESET_NOT_RESPONSE)
                        {
                            ReConnect();
                        }
                    }

                });
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Error when open connect loop: {ex}", LogType.Error);

                //Cố gắng kết nối lại
                ReConnect();
            }

        }

        public void CloseConnect()
        {
            isRunning = false;
            try
            {
                if (_EventBus != null)
                {
                    _EventBus.Dispose();
                }

                Close();
                Thread.Sleep(TIME_WAIT_CONNECT);
                //Thread.Sleep(500); //chờ 500ms mới quit app để đảm bảo thiết bị close đúng cách
            }
            catch { }
        }



        public void 
            SessionOnDataReceived(object sender, DataEventArgs e)
        {
            try
            {


                byte[] message = e.Data;
                int messageSize = e.Length;

                if (2 * messageSize < _MessageLength) return;

                Array.Resize(ref message, messageSize);
                string strMessage = ByteArrayToString(message, messageSize);
                strMessage = strMessage.Replace("\0", ""); //Bỏ ký tự NULL
                strMessage = strMessage.Replace("\r\n", ""); //Bỏ ký tự xuống dòng
                //_Logger.Write(_LogCategory, $"Received from IP {ServerIP}: [{strMessage}]", LogType.Debug);

                //thoi gian nhan ban tin
                lock (lockReceiveData)
                {
                    LastTimeReceiveData = DateTime.Now;
                }
                while(strMessage.Length >= _MessageLength)
                {
                    int _length = _MessageLength;
                    string msgProcess = "";
                    string _command = strMessage.Substring(2, 2).ToUpper();
                    if (_command == "0F")
                    {
                        _length = 16; //Bản tin trả về
                    }
                    msgProcess = strMessage.Substring(0, _length);
                    //Xóa bỏ bản tin vừa xử lý
                    strMessage = strMessage.Substring(_length);
                    //Bỏ qua bản tin trả về khi gửi lệnh ghi
                    if (_command == "0F") continue;

                    //_Logger.Write(_LogCategory, $"Processing message: [{msgProcess}]", LogType.Debug);
                    ProcessLogic(msgProcess);

                }

            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Process message received from  {this.ServerIP} Error: {ex}!", LogType.Error);
            }


        }
        /*
        public void SessionOnDataReceived(object sender, DataEventArgs e)
        {
            try
            {


                byte[] message = e.Data;
                int messageSize = e.Length;

                Array.Resize(ref message, messageSize);
                string strMessage = ByteArrayToString(message);

                strMessage = strMessage.Replace("\0", ""); //Bỏ ký tự NULL
                strMessage = strMessage.Replace("\r\n", ""); //Bỏ ký tự xuống dòng
                _Logger.Write(_LogCategory, $"Received from IP {ServerIP}: [{strMessage}]", LogType.Debug);


                //thoi gian nhan ban tin
                lock (lockReceiveData)
                {
                    LastTimeReceiveData = DateTime.Now;
                }
                //strMessage = strAvCache + strMessage;
                ////Xong thì xử lý cache
                //strAvCache = "";

                string[] separatingStrings = { ":" };
                string[] msgArry = strMessage.Split(separatingStrings, StringSplitOptions.RemoveEmptyEntries);

                for (int i = 0; i < msgArry.Length; i++)
                {

                    string msgProcess = msgArry[i];

                    //===============================================================================
                    //AnNH: Thêm đoạn này để kiểm tra xem bản tin đã đủ độ dài chưa
                    if (msgProcess.Length < _MessageLength) //Chưa đủ độ dài của msg
                    {
                        //strAvCache = msgArry[i];
                        //log.Info(string.Format("Cache: {0}", strAvCache));
                        continue;
                    }

                    //===============================================================================
                    ////For testing. Test xong nhớ comment lại dòng này và mở ra dòng bên dưới.
                    //OnReceivedMessage?.Invoke(strMessage);

                    //log.Info(string.Format("publish to rabbit: {0}", msgProcess));
                    // rabbit.Publish(msgProcess);

                    //Bỏ qua bản tin trả về khi gửi lệnh ghi
                    string _command = msgProcess.Substring(2, 2);
                    if (_command == "06") continue;

                    _Logger.Write(_LogCategory, $"[{msgProcess}]", LogType.Debug);
                    ProcessLogic(msgProcess);

                }

            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Process message received from  {this.ServerIP} Error: {ex}!", LogType.Error);
            }


        }
        */
        private Andon_MSG ParseModbusMessage(string data)
        {
            if (data.Length < 14)
            {
                _Logger.Write(_LogCategory, $"Error Messgage Received: {data}", LogType.Debug);
            }

            string _deviceId = data.Substring(0, 2);
            if (_deviceId == "00") return null;
            int _in1 = int.Parse(data.Substring(6, 4), System.Globalization.NumberStyles.HexNumber);
            int _in2 = int.Parse(data.Substring(10, 4), System.Globalization.NumberStyles.HexNumber);
            //if (_IsInvertInput)
            //{
            //    //_in1 = (_in1 == 0) ? 1 : 0;
            //    //_in2 = (_in2 == 0) ? 1 : 0;
            //}
            int _in3 = 0;//int.Parse(data.Substring(14, 4));
            int _in4 = 0;// int.Parse(data.Substring(12, 2), System.Globalization.NumberStyles.HexNumber);
            int _in5 = 0;// int.Parse(data.Substring(14, 2), System.Globalization.NumberStyles.HexNumber);
            int _in6 = 0;// int.Parse(data.Substring(16, 2), System.Globalization.NumberStyles.HexNumber);

            Andon_MSG msg = new Andon_MSG(this.ServerIP, DateTime.Now, MessageType.Andon, _deviceId, _in1, _in2, _in3, _in4, _in5, _in6);

            return msg;

        }
        private Andon_MSG ParseModbusDiscreteMessage(string data)
        {

            string _deviceId = data.Substring(0, 2);
            if (_deviceId == "00") return null;

            //Giá trị Input là vị trí cặp số 4
            int _input = int.Parse(data.Substring(6, 2), System.Globalization.NumberStyles.HexNumber);
            //Dịch qua BIT
            string _inputString = Convert.ToString(_input, 2).PadLeft(4, '0');
            int _in1 = int.Parse(_inputString.Substring(3,1));
            int _in2 = int.Parse(_inputString.Substring(2, 1));
            int _in3 = int.Parse(_inputString.Substring(1, 1));
            int _in4 = int.Parse(_inputString.Substring(0, 1));
            int _in5 = 0;// int.Parse(data.Substring(14, 2), System.Globalization.NumberStyles.HexNumber);
            int _in6 = 0;// int.Parse(data.Substring(16, 2), System.Globalization.NumberStyles.HexNumber);

            Andon_MSG msg = new Andon_MSG(this.ServerIP, DateTime.Now, MessageType.Andon, _deviceId, _in1, _in2, _in3, _in4, _in5, _in6);

            return msg;

        }
        /*
        private void ProcessResetReceived(string content)
        {
            try
            {
                //Kiểm tra xem nhận lệnh reset từ Node nào
                if (content.Length < 6) return;
                int _nodeId = Int32.Parse(content.Substring(4, 2), System.Globalization.NumberStyles.HexNumber);
                //int _code = Int32.Parse(content.Substring(6, 2), System.Globalization.NumberStyles.HexNumber);
                //_Logger.Write(_LogCategory, $"Checking is content of process reset received {_nodeId} from {this.ServerIP} with code = {_code}!", LogType.Debug);
                //Xác nhận tín hiệu Reset đã OK
                _Logger.Write(_LogCategory, $"Reset Node {_nodeId} from {this.ServerIP} done!", LogType.Info);
                //Cứ lệnh của Node nào bắn về chứng tỏ Node đó đã reset xong --> Xóa bỏ khỏi mảng Node2Reset
                ResetNode node = NodesReset.Find(n => n.NodeId == _nodeId);
                NodesReset.Remove(node);

            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Process reset message {content} from {this.ServerIP} Error: {ex}!", LogType.Debug);
            }

            return;
        }
        */

        public void ProcessLogic(string _rawMessage)
        {
            //string _rawMessage = JsonConvert.SerializeObject(message);
            try
            {
                //Phân tích msg ở đây

                //Andon_MSG message = ParseModbusMessage(_rawMessage);
                Andon_MSG message = ParseModbusDiscreteMessage(_rawMessage);

                if (message == null) return;

                Nodes[int.Parse(message.Body.DeviceId)] = message;

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

                if (_EventBus != null && _EventBus.IsConnected)
                {
                    _EventBus.Publish<Andon_MSG>(message);
                }
                else
                {
                    _Logger.Write(_LogCategory, $" [{_rawMessage}]", LogType.Error, "_Error_" + this.ServerIP);
                }
                //Ghi logs raws
                WriteRawsData(message);

            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Process logic message {_rawMessage} from  {this.ServerIP} Error: {ex}!", LogType.Error);
            }

        }
        
        private void WriteRawsData(Andon_MSG message)
        {
            _Rawer.Write(_LogCategory, $"{JsonConvert.SerializeObject(message)}", LogType.Debug, "Raw_" + this.ServerIP);
        }
        private string ByteArrayToString(byte[] ba)
        {
            string ret = "";
            for(int _id = 0; _id < ba.Length; _id++)
            {
                ret += $"{ba[_id]:X2}";
            }
            //if (!ret.StartsWith(":")) ret = ":" + ret;
            if (!ret.EndsWith("\r\n")) ret += "\r\n";

            return ret;
        }
        private string ByteArrayToString(byte[] ba, int size)
        {
            //StringBuilder hex = new StringBuilder(ba.Length * 2);
            StringBuilder hex = new StringBuilder(size * 2);

            foreach (byte b in ba)
                hex.AppendFormat("{0:x2}", b);
            return hex.ToString();
        }
        void SessionConnected(object sender, EventArgs e)
        {
            _Logger.Write(_LogCategory, $"Connected to {ServerIP}", LogType.Info);
        }

        void SessionClosed(object sender, EventArgs e)
        {
            _Logger.Write(_LogCategory, $"Session to {ServerIP} closed!", LogType.Info);
            
        }

        void SessionError(object sender, SuperSocket.ClientEngine.ErrorEventArgs e)
        {
            _Logger.Write(_LogCategory, $"Error: {e}", LogType.Error);
        }

        public override int ReceiveBufferSize { get => base.ReceiveBufferSize; set => base.ReceiveBufferSize = value; }

        public override void Close()
        {
            _TimerPingCommand.Stop();
            _TimerSendCommand.Stop();
            base.Close();
        }

        public override void Connect(EndPoint remoteEndpoint)
        {
            try
            {
                base.Connect(remoteEndpoint);
            }
            catch (Exception ex)
            {

            }
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string ToString()
        {
            return base.ToString();
        }

        public override bool TrySend(ArraySegment<byte> segment)
        {
            return base.TrySend(segment);
        }

        public override bool TrySend(IList<ArraySegment<byte>> segments)
        {
            return base.TrySend(segments);
        }

        protected override bool IsIgnorableException(Exception e)
        {
            return base.IsIgnorableException(e);
        }

        protected override void OnClosed()
        {
            base.OnClosed();
        }

        protected override void OnConnected()
        {
            base.OnConnected();
        }

        protected override void OnDataReceived(byte[] data, int offset, int length)
        {
            base.OnDataReceived(data, offset, length);
        }

        protected override void OnError(Exception e)
        {
            base.OnError(e);
            _Logger.Write(_LogCategory, $"Error: {e}", LogType.Error);

        }

        protected override void OnGetSocket(SocketAsyncEventArgs e)
        {
            base.OnGetSocket(e);
        }

        protected override void SendInternal(PosList<ArraySegment<byte>> items)
        {
            base.SendInternal(items);
        }

        protected override void SetBuffer(ArraySegment<byte> bufferSegment)
        {
            base.SetBuffer(bufferSegment);
        }

        protected override void SocketEventArgsCompleted(object sender, SocketAsyncEventArgs e)
        {
            base.SocketEventArgsCompleted(sender, e);
        }
    }
}
