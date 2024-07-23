using Avani.Andon.Edge.Logic;
using System;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Windows.Forms;

namespace Avani.Andon.Edge.Test
{
    public partial class Form1 : Form
    {
        private bool _IsRunning = false;
        private Logic.MainApp _AppLogic { get; set; }

        System.Timers.Timer timer = new System.Timers.Timer();
        public Form1()
        {
            InitializeComponent();
        }
        private void Form1_Load(object sender, EventArgs e)
        {
            string configs = "";
            foreach(string key in ConfigurationManager.AppSettings.AllKeys)
            {
                configs += $"{key}: {ConfigurationManager.AppSettings[key]}{Environment.NewLine}";
            }
            txtConfig.Text = configs;
            _AppLogic = new Logic.MainApp();
        }
        private void btnCommand_Click(object sender, EventArgs e)
        {
            //IBus _EventBus = RabbitHutch.CreateBus($"host=27.72.29.38:5672;virtualHost=/;username=genset;password=genset");


            if (_IsRunning)
            {
                Stop();
            }
            else
            {
                Start();
            }
            return;
 
        }

        private void Start()
        {
            _IsRunning = true;
            btnCommand.Text = "Stop";
            txtMessage.Text = "";
            _AppLogic.Start();
            //timer.Interval = 1;
            //timer.Elapsed += Timer_Elapsed;
            //timer.Start();
        }

        private void Timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            txtMessage.Text += "A";
        }

        private void Stop()
        {
            _IsRunning = false;
            btnCommand.Text = "Start";
            _AppLogic.Stop();
            //timer.Stop();
        }

    }
}
