using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
// ReSharper disable CheckNamespace
// ReSharper disable UnusedMember.Global

namespace SqlBulkTools.TestCommon.Model
{
    public class CustomColumnMappingTest
    {
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        [Key]
        public int NaturalIdTest { get; set; }

        [Column("ColumnX"), StringLength(256)]
        public string ColumnXIsDifferent { get; set; }

        [Column("ColumnY")]
        public int ColumnYIsDifferentInDatabase { get; set; }
    }
}
