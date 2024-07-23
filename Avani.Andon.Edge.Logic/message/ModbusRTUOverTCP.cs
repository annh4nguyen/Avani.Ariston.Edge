using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Avani.Andon.Edge.Logic
{
    public class ModbusRTUOverTCP
    {
        #region Writes

        /// <summary>
        /// Function 06 (06hex)  Write Single Register
        /// </summary>
        /// <param name="slaveAddress">Slave Address</param>
        /// <param name="startAddress">Starting Address</param>
        /// <param name="function">Function</param>
        /// <param name="values">Data</param>
        /// <returns>Byte Array</returns>
        public static byte[] WriteSingleRegisterMsg(byte slaveAddress, ushort startAddress, byte function, byte[] values)
        {
            byte[] frame = new byte[7];                     // Message size
            frame[0] = slaveAddress;                        // Slave address
            frame[1] = function;                            // Function code            
            frame[2] = (byte)(startAddress >> 8);           // Register Address Hi
            frame[3] = (byte)startAddress;                  // Register Address lo
            Array.Copy(values, 0, frame, 4, values.Length); // Write Data
            byte _lrc = calculateLRC(frame);
            frame[frame.Length - 1] = _lrc;               //LRC

            //byte[] crc = CalculateCRC(frame);          // Calculate CRC
            //frame[frame.Length - 2] = crc[0];               //Error Check Lo
            //frame[frame.Length - 1] = crc[1];               //Error Check Hi

            return frame;
        }

        public static byte[] WriteMultiCoilsMsg(byte slaveAddress, ushort startAddress, byte quantityAddress, byte function, byte value)
        {
            byte[] frame = new byte[10];                    // Message size
            frame[0] = slaveAddress;                        // Slave address
            frame[1] = function;                            // Function code            
            frame[2] = (byte)(startAddress >> 8);           // Register Address Hi
            frame[3] = (byte)startAddress;                  // Register Address lo
            frame[4] = (byte)(quantityAddress >> 8);        // Quantity Hi
            frame[5] = (byte)quantityAddress;               // Quatity lo
            frame[6] = 1;                                   // Bytes sent command bytes
            frame[7] = value;                                   // Value

            byte[] crc = CalculateCRC(frame);          // Calculate CRC
            frame[frame.Length - 2] = crc[0];               //Error Check Lo
            frame[frame.Length - 1] = crc[1];               //Error Check Hi

            return frame;
        }


        #endregion

        #region Reads

        /// <summary>
        /// Function 02 (02hex) Read Discrete Input
        /// </summary>
        /// <param name="slaveAddress">Slave Address</param>
        /// <param name="startAddress">Starting Address</param>
        /// <param name="function">Function</param>
        /// <param name="numberOfPoints">Quantity of inputs</param>
        /// <returns>Byte Array</returns>
        /// 

        public static byte[] ReadDiscreteInputsMsg(byte slaveAddress, ushort startAddress, byte function, uint numberOfPoints)
        {
            byte[] frame = new byte[8];
            frame[0] = slaveAddress;			    // Slave Address
            frame[1] = function;				    // Function             
            frame[2] = (byte)(startAddress >> 8);	// Starting Address High
            frame[3] = (byte)startAddress;		    // Starting Address Low            
            frame[4] = (byte)(numberOfPoints >> 8);	// Quantity of Registers High
            frame[5] = (byte)numberOfPoints;		// Quantity of Registers Low
            
            byte[] crc = CalculateCRC(frame);  // Calculate CRC.

            frame[frame.Length - 2] = crc[0];       // Error Check Low
            frame[frame.Length - 1] = crc[1];       // Error Check High
            return frame;
        }
        

        /*
        public static string ReadDiscreteInputsMsg(byte slaveAddress, ushort startAddress, byte function, uint numberOfPoints)
        {
            string ret = $"{slaveAddress:X2}{function:X2}{startAddress:X4}{numberOfPoints:X4}";
            byte[] frame = new byte[8];
            frame[0] = slaveAddress;			    // Slave Address
            frame[1] = function;				    // Function             
            frame[2] = (byte)(startAddress >> 8);	// Starting Address High
            frame[3] = (byte)startAddress;		    // Starting Address Low            
            frame[4] = (byte)(numberOfPoints >> 8);	// Quantity of Registers High
            frame[5] = (byte)numberOfPoints;        // Quantity of Registers Low

            byte[] crc = CalculateCRC(frame);  // Calculate CRC.
            frame[frame.Length - 2] = crc[0];       // Error Check Low
            frame[frame.Length - 1] = crc[1];       // Error Check High

            ret += $"{crc[0]:X2}{crc[1]:X2}";

            return ret;
        }
        */

        /// <summary>
        /// Function 03 (03hex) Read Holding Registers
        /// </summary>
        /// <param name="slaveAddress">Slave Address</param>
        /// <param name="startAddress">Starting Address</param>
        /// <param name="function">Function</param>
        /// <param name="numberOfPoints">Quantity of inputs</param>
        /// <returns>Byte Array</returns>
        public static byte[] ReadHoldingRegistersMsg(byte slaveAddress, ushort startAddress, byte function, uint numberOfPoints)
        {
            byte[] frame = new byte[8];
            frame[0] = slaveAddress;			    // Slave Address
            frame[1] = function;				    // Function             
            frame[2] = (byte)(startAddress >> 8);	// Starting Address High
            frame[3] = (byte)startAddress;		    // Starting Address Low            
            frame[4] = (byte)(numberOfPoints >> 8);	// Quantity of Registers High
            frame[5] = (byte)numberOfPoints;		// Quantity of Registers Low
            byte[] crc = CalculateCRC(frame);  // Calculate CRC.
            frame[frame.Length - 2] = crc[0];       // Error Check Low
            frame[frame.Length - 1] = crc[1];       // Error Check High
            return frame;
        }

        /// <summary>
        /// Read the binary contents of holding registers in the slave.
        /// </summary>
        /// <param name="startAddress">Starting Address</param>
        /// <param name="numberOfPoints">Quantity of inputs</param>
        /// <returns>Registers /returns>
        //public static List<Register> ReadHoldingRegisters(ushort startAddress, uint numberOfPoints)
        //{
        //    const byte function = 3;
        //    if (serialPort1.IsOpen)
        //    {
        //        byte[] frame = ReadHoldingRegistersMsg(slaveAddress, startAddress, function, numberOfPoints);
        //        serialPort1.Write(frame, 0, frame.Length);
        //        Thread.Sleep(100); // Delay 100ms
        //        if (serialPort1.BytesToRead >= 5)
        //        {
        //            byte[] bufferReceiver = new byte[serialPort1.BytesToRead];
        //            serialPort1.Read(bufferReceiver, 0, serialPort1.BytesToRead);
        //            serialPort1.DiscardInBuffer();

        //            // Process data.
        //            byte[] data = new byte[bufferReceiver.Length - 5];
        //            Array.Copy(bufferReceiver, 3, data, 0, data.Length);
        //            UInt16[] result = Word.ByteToUInt16(data);
        //            for (int i = 0; i < result.Length; i++)
        //            {
        //                Registers[i].Value = result[i];
        //            }
        //        }
        //    }
        //    return Registers;
        //}

        #endregion

        public static byte calculateLRC(byte[] bytes)
        {
            int LRC = 0;
            for (int i = 0; i < bytes.Length; i++)
            {
                LRC -= bytes[i];
            }
            return (byte)LRC;
        }
        public bool CheckCRC(byte[] response, int size, bool _Modbus_CheckCRC)
        {

            if (!_Modbus_CheckCRC) return true;

            //Perform a basic CRC check:

            bool _ret = false;

            byte[] CRC = new byte[2];
            GetCRC(response, size, ref CRC);
            //if (CRC[0] == response[response.Length - 2] && CRC[1] == response[response.Length - 1])
            if (CRC[0] == response[size - 2] && CRC[1] == response[size - 1])
            {
                _ret = true;
            }

            return _ret;
        }

        private void GetCRC(byte[] message, int size, ref byte[] CRC)
        {
            //Function expects a modbus message of any length as well as a 2 byte CRC array in which to 
            //return the CRC values:

            ushort CRCFull = 0xFFFF;
            byte CRCHigh = 0xFF, CRCLow = 0xFF;
            char CRCLSB;

            for (int i = 0; i < size - 2; i++)
            {
                CRCFull = (ushort)(CRCFull ^ message[i]);

                for (int j = 0; j < 8; j++)
                {
                    CRCLSB = (char)(CRCFull & 0x0001);
                    CRCFull = (ushort)((CRCFull >> 1) & 0x7FFF);

                    if (CRCLSB == 1)
                        CRCFull = (ushort)(CRCFull ^ 0xA001);
                }
            }
            CRC[1] = CRCHigh = (byte)((CRCFull >> 8) & 0xFF);
            CRC[0] = CRCLow = (byte)(CRCFull & 0xFF);
        }

        /// <summary>
        /// CRC Calculation 
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private static byte[] CalculateCRC(byte[] data)
        {
            ushort CRCFull = 0xFFFF; // Set the 16-bit register (CRC register) = FFFFH.
            char CRCLSB;
            byte[] CRC = new byte[2];
            for (int i = 0; i < (data.Length) - 2; i++)
            {
                CRCFull = (ushort)(CRCFull ^ data[i]); // 

                for (int j = 0; j < 8; j++)
                {
                    CRCLSB = (char)(CRCFull & 0x0001);
                    CRCFull = (ushort)((CRCFull >> 1) & 0x7FFF);

                    if (CRCLSB == 1)
                        CRCFull = (ushort)(CRCFull ^ 0xA001);
                }
            }
            CRC[1] = (byte)((CRCFull >> 8) & 0xFF);
            CRC[0] = (byte)(CRCFull & 0xFF);
            return CRC;
        }

    }
}
