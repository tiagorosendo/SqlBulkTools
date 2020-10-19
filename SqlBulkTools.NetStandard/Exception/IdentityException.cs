using System.Runtime.CompilerServices;
// ReSharper disable CheckNamespace
// ReSharper disable UnusedMember.Global

[assembly: InternalsVisibleTo("SqlBulkTools.UnitTests")]
[assembly: InternalsVisibleTo("SqlBulkTools.IntegrationTests")]
namespace SqlBulkTools
{
    internal class IdentityException : SqlBulkToolsException
    {
        public IdentityException(string message) : base(message + " SQLBulkTools requires the SetIdentityColumn method " +
                                                            "to be configured if an identity column is being used. Please reconfigure your setup and try again.")
        {
        }
    }
}
