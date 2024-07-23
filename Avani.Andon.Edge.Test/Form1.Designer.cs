
namespace Avani.Andon.Edge.Test
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.label1 = new System.Windows.Forms.Label();
            this.txtConfig = new System.Windows.Forms.TextBox();
            this.btnCommand = new System.Windows.Forms.Button();
            this.txtMessage = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(9, 7);
            this.label1.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(37, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Config";
            // 
            // txtConfig
            // 
            this.txtConfig.Location = new System.Drawing.Point(9, 28);
            this.txtConfig.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.txtConfig.Multiline = true;
            this.txtConfig.Name = "txtConfig";
            this.txtConfig.ReadOnly = true;
            this.txtConfig.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.txtConfig.Size = new System.Drawing.Size(472, 175);
            this.txtConfig.TabIndex = 1;
            // 
            // btnCommand
            // 
            this.btnCommand.Location = new System.Drawing.Point(400, 207);
            this.btnCommand.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.btnCommand.Name = "btnCommand";
            this.btnCommand.Size = new System.Drawing.Size(80, 29);
            this.btnCommand.TabIndex = 2;
            this.btnCommand.Text = "Start";
            this.btnCommand.UseVisualStyleBackColor = true;
            this.btnCommand.Click += new System.EventHandler(this.btnCommand_Click);
            // 
            // txtMessage
            // 
            this.txtMessage.Location = new System.Drawing.Point(9, 241);
            this.txtMessage.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.txtMessage.Multiline = true;
            this.txtMessage.Name = "txtMessage";
            this.txtMessage.ReadOnly = true;
            this.txtMessage.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.txtMessage.Size = new System.Drawing.Size(472, 237);
            this.txtMessage.TabIndex = 3;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(9, 218);
            this.label2.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(37, 13);
            this.label2.TabIndex = 0;
            this.label2.Text = "Result";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(489, 487);
            this.Controls.Add(this.txtMessage);
            this.Controls.Add(this.btnCommand);
            this.Controls.Add(this.txtConfig);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.Name = "Form1";
            this.Text = "Test Form for Andon Edge";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox txtConfig;
        private System.Windows.Forms.Button btnCommand;
        private System.Windows.Forms.TextBox txtMessage;
        private System.Windows.Forms.Label label2;
    }
}

