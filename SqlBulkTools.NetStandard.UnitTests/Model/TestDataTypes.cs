using Microsoft.SqlServer.Types;
using System;

namespace SqlBulkTools.TestCommon.Model
{
    public class TestDataType
    {
        public float FloatTest { get; set; }
        public double FloatTest2 { get; set; }
        public decimal DecimalTest { get; set; }
        public decimal MoneyTest { get; set; }
        public decimal SmallMoneyTest { get; set; }
        public float RealTest { get; set; }
        public decimal NumericTest { get; set; }
        public DateTime DateTimeTest { get; set; }
        public DateTime DateTime2Test { get; set; }
        public DateTime SmallDateTimeTest { get; set; }
        public DateTime DateTest { get; set; }
        public TimeSpan TimeTest { get; set; }
        public Guid GuidTest { get; set; }
        public string TextTest { get; set; }
        public byte[] VarBinaryTest { get; set; }
        public byte[] BinaryTest { get; set; }
        public byte TinyIntTest { get; set; }
        public long BigIntTest { get; set; }
        public string CharTest { get; set; }
        public byte[] ImageTest { get; set; }
        public string NTextTest { get; set; }
        public string NCharTest { get; set; }
        public string XmlTest { get; set; }
        public SqlGeometry TestSqlGeometry { get; set; }
        public SqlGeography TestSqlGeography { get; set; }
    }
}
