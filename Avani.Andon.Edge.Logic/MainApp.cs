using Avani.Helper;
using EasyNetQ;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using System.Net;
using System.Text;

namespace Avani.Andon.Edge.Logic
{
    public class MainApp
    {
        #region props
        private Log _Logger;
        private readonly string _LogCategory = "MainApp";

        //SERVER or CLIENT
        private string _WORKING_MODE = ConfigurationManager.AppSettings["WORKING_MODE"];


        private TCPServer _Server = null;
        private int _ServerPort = int.Parse(ConfigurationManager.AppSettings["ServerPort"]);

        //Gateway
        string GatewayIPs = ConfigurationSettings.AppSettings["DeviceIP"].Trim();
        int GatewayPort = Convert.ToInt32(ConfigurationSettings.AppSettings["DevicePort"].Trim());
        //public static int ATCommandPort = Convert.ToInt32(ConfigurationSettings.AppSettings["ATCommandPort"].Trim());//Port
        public static int RESET_AFTER_NOT_RECEIVE_DATA = Convert.ToInt32(ConfigurationSettings.AppSettings["RESET_AFTER_NOT_RECEIVE_DATA"].Trim());//Time
        /*
        private static IBus _EventBus = null;
        private string _RabbitMQHost = ConfigurationManager.AppSettings["RabbitMQ.Host"];
        private string _RabbitMQVirtualHost = ConfigurationManager.AppSettings["RabbitMQ.VirtualHost"];
        private string _RabbitMQUser = ConfigurationManager.AppSettings["RabbitMQ.User"];
        private string _RabbitMQPassword = ConfigurationManager.AppSettings["RabbitMQ.Password"];
        private string _CustomerID = ConfigurationManager.AppSettings["CustomerID"];

        //private Timer _TimerRequest = new Timer();
        */

        private int _RequestInterval = 1000 * int.Parse(ConfigurationManager.AppSettings["request_interval"]);
        System.Timers.Timer timerCheckNoData = new System.Timers.Timer();

        private List<TCPSocketClient> Gateways = new List<TCPSocketClient>();

        Thread threadStartGateway;

        bool isInitForPublishRabbit = false;



        System.Timers.Timer timerCheckDisconnect = new System.Timers.Timer();


        #endregion
        #region constructors
        public MainApp()
        {
            _Logger = Avani.Andon.Edge.Logic.Helper.GetLog();
        }
        #endregion
        #region public methods
        public void Start()
        {
            try
            {

                _Logger.Write(_LogCategory, $"Main Thread started!", LogType.Info);

                if (_WORKING_MODE == "SERVER")
                {
                    //Start as Server role
                    _Server = new TCPServer(_ServerPort);
                    _Server.Start();
                }
                else
                {
                    //Start as client Role
                    SetupGateway();
                    OpenGateways();
                    //ListenCommand();
                }



            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, ex);
                this.Stop();
            }
        }

        public void Stop()
        {
            try
            {
                if (_WORKING_MODE == "SERVER")
                {
                    _Server.Stop();
                    _Server = null;
                }
                else
                {
                    CloseGateways();
                }
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, ex);
            }
        }



        private void SetupGateway()
        {
            try
            {
                _Logger.Write(_LogCategory, $"Setup gateways for config: {GatewayIPs}!", LogType.Debug);
                string[] _gateways = GatewayIPs.Split(new string[] { ";" }, StringSplitOptions.RemoveEmptyEntries);
                for (int i = 0; i < _gateways.Length; i++)
                {
                    string[] gateways = _gateways[i].Split(new string[] { "#" }, StringSplitOptions.RemoveEmptyEntries);
                    string _ip = gateways[0];
                    int _node = int.Parse(gateways[1]);

                    //_Logger.Write(_LogCategory, $"{i + 1} - gateway {_ip} - port {GatewayPort}  - nodes {_node}", LogType.Debug);
                    TCPSocketClient _client = new TCPSocketClient(i + 1, _ip, GatewayPort, _node);
                    //_Logger.Write(_LogCategory, $"{i + 1} - gateway {gateways[0]} - port {GatewayPort} - nodes {gateways[1]}", LogType.Debug);
                    Gateways.Add(_client);
                }
                //string[] arrIP = GatewayIPs.Split(';');

                //for (int i = 0; i < arrIP.Length; i++)
                //{
                //};

                _Logger.Write(_LogCategory, $"Setup gateways {Gateways.Count}...!", LogType.Debug);

            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Error when setup Gateway: {ex.Message}", LogType.Error);
            }
        }

        private void OpenGateways()
        {
            try
            {

                //khoi tao rabbit de publish
                //if (!isInitForPublishRabbit)
                //{
                //    rabbit.InitForPublish();
                //    isInitForPublishRabbit = true;
                //}

                _Logger.Write(_LogCategory, $"Open gateways {Gateways.Count}", LogType.Debug);


                threadStartGateway = new Thread(() =>
                {
                    Parallel.For(0, Gateways.Count,
                        i =>
                        {
                            Gateways[i].OpenConnectLoop();
                        });
                });

                threadStartGateway.Start();

                //Khởi tạo thằng lắng nghe để reset Service nếu không nhận được dữ liệu quá thời gain
                timerCheckNoData.Interval = _RequestInterval;
                timerCheckNoData.Elapsed += _TimerCheckForNoData_Elapsed;
                timerCheckNoData.Start();

            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Error when Open Gateway: {ex.Message}", LogType.Error);
            }
        }
        private void _TimerCheckForNoData_Elapsed(object sender, ElapsedEventArgs e)
        {
            bool isReset = false;
            DateTime checkTime = DateTime.Now;
            double _not_receive_duration = 0;
            string _ip = "";
            foreach (TCPSocketClient client in Gateways)
            {
                _not_receive_duration = (checkTime - client.LastTimeReceiveData).TotalSeconds;
                if (_not_receive_duration > RESET_AFTER_NOT_RECEIVE_DATA)
                {
                    _ip = client.ServerIP;
                    isReset = true;
                    break;
                }
            }


            if (isReset)
            {
                _Logger.Write(_LogCategory, $"Restart Service after {_not_receive_duration} second not receive data at {_ip}!", LogType.Debug);
                ReStartService();
            }

        }

        private void ReStartService()
        {
            try
            {
                Stop();
                Thread.Sleep(60);
                Start();
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Error when Restart Service: {ex.Message}", LogType.Error);
            }

        }

/*
private void ResetGateway(TCPSocketClient client)
{
    try
    {
        //Chỗ này kiểm tra xem bao nhiều lần rồi không nhận được dữ liệu
        //Nếu chưa nhận được thì Reset thiết bị ngay
        _Logger.Write(_LogCategory, $"Start reset gateway {client.Id}!", LogType.Debug);

        TCPSocketClient _ATClient = new TCPSocketClient(0, client.ServerIP, ATCommandPort);
        IPEndPoint iPEndPoint = new IPEndPoint(IPAddress.Parse(client.ServerIP), ATCommandPort);
        _ATClient.Connect(iPEndPoint);
        _Logger.Write(_LogCategory, $"Check connect status: {_ATClient.IsConnected}!", LogType.Debug);
        if (_ATClient.IsConnected)
        {
            _Logger.Write(_LogCategory, $"Start send command to reset gateway {client.Id}!", LogType.Debug);
            string message = "AT REBOOT \r\n";
            byte[] messageByte = Encoding.ASCII.GetBytes(message);
            _ATClient.Send(messageByte, 0, message.Length);
            Thread.Sleep(100);
            _ATClient.Close();
            Thread.Sleep(100);
            _Logger.Write(_LogCategory, $"Reset gateway {client.Id} done!", LogType.Debug);
        }
    }
    catch (Exception e)
    {
        _Logger.Write(_LogCategory, $"Error when reset gateway {client.Id}!", LogType.Error);
    }

}
*/
private void CloseGateways()
        {

            foreach (TCPSocketClient _clent in Gateways)
            {
                _clent.CloseConnect();
            }
            //rabbit.ShutdownRabbit();

        }
        #endregion
        #region private methods
        /*
        private void ListenCommand()
        {
            _Logger.Write(_LogCategory, $"Start listenning to {_RabbitMQHost}", LogType.Info);
            if (_EventBus == null || !_EventBus.IsConnected)
            {
                _Logger.Write(_LogCategory, $"Connecting to RabbitMQ {_RabbitMQHost}", LogType.Info);
                _EventBus = RabbitHutch.CreateBus($"host={_RabbitMQHost};virtualHost={_RabbitMQVirtualHost};username={_RabbitMQUser};password={_RabbitMQPassword}");
                if (_EventBus.IsConnected || _EventBus.Advanced.IsConnected)
                {
                    _Logger.Write(_LogCategory, $"Connected to RabbitMQ {_RabbitMQHost} for Listenning command!", LogType.Info);

                    _EventBus.Subscribe<AndonControlMessage>(_CustomerID, msg => {
                        ProccessCommand(msg);
                    });
                }
            }
        }
        private void ProccessCommand(AndonControlMessage message)
        {
            try
            {
                string NodeIds = message.Nodes;
                _Logger.Write(_LogCategory, $"Check to reset Line {message.LineId}: {message.Nodes}!", LogType.Debug);
                string[] arrayNodes = NodeIds.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);

                if (arrayNodes.Length <= 0) return;

                //Kiểm tra Node xem nó ở line nào
                int _nodeDefaultId = int.Parse(arrayNodes[0]);
                int _gatewayIndex = (int) (_nodeDefaultId / _NUMBER_NODE_IN_GATEWAY);

                _Logger.Write(_LogCategory, $"Check Gateway {_gatewayIndex} to reset!", LogType.Debug);

                TCPSocketClient _client = Gateways[_gatewayIndex];
                foreach(string node in arrayNodes)
                {
                    int _nodeId = int.Parse(node);
                    _nodeId = _nodeId % _NUMBER_NODE_IN_GATEWAY; //Lấy ra đúng ID của thiết bị
                    _client.QueuingCommand(_nodeId);
                }
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Error when process command for {message.LineId}: {message.Nodes} ~ {ex}", LogType.Error);
            }
        }
        */
        #endregion
    }
}
