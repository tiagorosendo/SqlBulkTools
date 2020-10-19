using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
// ReSharper disable CheckNamespace
// ReSharper disable UnusedMember.Global

namespace SqlBulkTools.TestCommon.Model
{
    [Table("SchemaTest")]
    public class SchemaTest1
    {
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        [Key]
        public int Id { get; set; }

        public string ColumnB { get; set; }
    }
}