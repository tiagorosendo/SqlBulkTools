using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
// ReSharper disable CheckNamespace
// ReSharper disable UnusedMember.Global

namespace SqlBulkTools.TestCommon.Model
{
    public class ReservedColumnNameTest
    {
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        [Key]
        public int Id { get; set; }

        public int Key { get; set; }
    }
}