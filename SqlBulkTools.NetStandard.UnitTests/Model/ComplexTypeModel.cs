using System;
using System.ComponentModel.DataAnnotations.Schema;
// ReSharper disable CheckNamespace
// ReSharper disable UnusedMember.Global

namespace SqlBulkTools.TestCommon.Model
{

    public class ComplexTypeModel
    {
        public int Id { get; set; }

        public EstimatedStats MinEstimate { get; set; }

        public EstimatedStats AverageEstimate { get; set; }

        public double SearchVolume { get; set; }

        public double Competition { get; set; }
    }


    [ComplexType]
    public class EstimatedStats
    {
        public EstimatedStats()
        {
            CreationDate = DateTime.UtcNow;
        }

        public double? TotalCost { get; set; }

        public DateTime CreationDate { get; set; }
    }
}
