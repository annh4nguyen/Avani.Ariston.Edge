using System;
using System.Net.Mail;

namespace Avani.Helper
{
    public class EmailClient
    {
        public string SmtpHost { get; set; }
        public int SmtpPort { get; set; }
        public int SmtpTimeout { get; set; } = 100000;
        public bool SmtpSSL { get; set; }
        public string SmtpUser { get; set; }
        public string SmtpPassword { get; set; }
        public EmailClient(string smtpHost, int smtpPort, int smtpTimeout, bool smtpSSL, string smtpUser, string smtpPassword)
        {
            this.SmtpHost = smtpHost;
            this.SmtpPort = smtpPort;
            this.SmtpTimeout = smtpTimeout;
            this.SmtpSSL = smtpSSL;
            this.SmtpUser = smtpUser;
            this.SmtpPassword = smtpPassword;
        }
        /// <summary>
        /// Gửi email
        /// </summary>
        /// <param name="mailMessage">Nội dung email</param>
        public void Send(MailMessage mailMessage)
        {
            try
            {
                using(SmtpClient client = new SmtpClient() { 
                    Host = this.SmtpHost,
                    Port = this.SmtpPort,
                    Timeout = this.SmtpTimeout,
                    EnableSsl = this.SmtpSSL,
                    DeliveryMethod = SmtpDeliveryMethod.Network,
                    UseDefaultCredentials = false,
                    Credentials = new System.Net.NetworkCredential(this.SmtpUser, this.SmtpPassword)
                })
                {
                    client.Send(mailMessage);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public void SendAsync(MailMessage mailMessage)
        {
            try
            {
                using (SmtpClient client = new SmtpClient()
                {
                    Host = this.SmtpHost,
                    Port = this.SmtpPort,
                    Timeout = this.SmtpTimeout,
                    EnableSsl = this.SmtpSSL,
                    DeliveryMethod = SmtpDeliveryMethod.Network,
                    UseDefaultCredentials = false,
                    Credentials = new System.Net.NetworkCredential(this.SmtpUser, this.SmtpPassword)
                })
                {
                    client.SendAsync(mailMessage, null);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}
