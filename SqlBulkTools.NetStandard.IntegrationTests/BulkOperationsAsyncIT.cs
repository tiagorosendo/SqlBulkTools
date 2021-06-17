using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using SqlBulkTools.Enumeration;
using SqlBulkTools.TestCommon.Model;
using SqlBulkTools.TestCommon;
using SqlBulkTools.IntegrationTests.Data;
using Xunit;
using AutoFixture;

namespace SqlBulkTools.IntegrationTests
{
    [Collection("IntegrationTests")]
    public class BulkOperationsAsyncIt
    {
        private const int _repeatTimes = 1;

        private readonly BookRandomizer _randomizer = new BookRandomizer();
        private readonly DataAccess _dataAccess = new DataAccess();
        private List<Book> _bookCollection;

        [Fact]
        public async Task SqlBulkTools_BulkDeleteWithSelectedColumns_TestIdentityOutput()
        {

            await BulkDeleteAsync(_dataAccess.GetBookList());

            using (var conn = new SqlConnection(_dataAccess.ConnectionString))
            using (var command = new SqlCommand(
                "DBCC CHECKIDENT ('[dbo].[Books]', RESEED, 10);", conn)
            {
                CommandType = CommandType.Text
            })
            {
                conn.Open();
                await command.ExecuteNonQueryAsync();
            }

            var books = _randomizer.GetRandomCollection(30);
            await BulkInsertAsync(books);

            var bulk = new BulkOperations();

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            BatchSize = 5000
                        })
                        .AddColumn(x => x.ISBN)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            var test = books.First();

            Assert.True(test.Id == 10 || test.Id == 11);

            // Reset identity seed back to default
            _dataAccess.ReseedBookIdentity(0);
        }

        [Fact]
        public async Task SqlBulkTools_BulkInsertAsync()
        {
            const int rows = 1000;

            await BulkDeleteAsync(_dataAccess.GetBookList());

            _bookCollection = new List<Book>();
            _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
            var results = new List<long>();

            Trace.WriteLine("Testing BulkInsertAsync with " + rows + " rows");

            for (var i = 0; i < _repeatTimes; i++)
            {
                var time = await BulkInsertAsync(_bookCollection);
                results.Add(time);
            }
            var avg = results.Average(l => l);
            Trace.WriteLine("Average result (" + _repeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            Assert.Equal(rows * _repeatTimes, _dataAccess.GetBookCount());
        }

        [Fact]
        public async Task SqlBulkTools_BulkInsertOrUpdateAsync()
        {
            const int rows = 500, newRows = 500;

            await BulkDeleteAsync(_dataAccess.GetBookList());

            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            var results = new List<long>();

            Trace.WriteLine("Testing BulkInsertOrUpdateAsync with " + (rows + newRows) + " rows");

            for (var i = 0; i < _repeatTimes; i++)
            {
                await BulkInsertAsync(_bookCollection);

                // Update some rows
                for (var j = 0; j < 200; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;
                }

                // Add new rows
                _bookCollection.AddRange(_randomizer.GetRandomCollection(newRows));


                var time = await BulkInsertOrUpdateAsync(_bookCollection);
                results.Add(time);

                Assert.Equal(rows + newRows, _dataAccess.GetBookCount());

            }

            var avg = results.Average(l => l);
            Trace.WriteLine("Average result (" + _repeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");
        }

        [Fact]
        public async Task SqlBulkTools_BulkUpdateAsync()
        {
            const int rows = 1000;

            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());

            await BulkDeleteAsync(_dataAccess.GetBookList());

            var results = new List<long>();

            Trace.WriteLine("Testing BulkUpdateAsync with " + rows + " rows");

            for (var i = 0; i < _repeatTimes; i++)
            {

                _bookCollection = _randomizer.GetRandomCollection(rows);
                await BulkInsertAsync(_bookCollection);

                // Update half the rows
                for (var j = 0; j < rows / 2; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;

                }

                var time = await BulkUpdateAsync(_bookCollection);
                results.Add(time);

                var testUpdate = _dataAccess.GetBookList().First();
                Assert.Equal(_bookCollection[0].Price, testUpdate.Price);
                Assert.Equal(_bookCollection[0].Title, testUpdate.Title);
                Assert.Equal(_dataAccess.GetBookCount(), _bookCollection.Count);

                await BulkDeleteAsync(_bookCollection);
            }
            var avg = results.Average(l => l);
            Trace.WriteLine("Average result (" + _repeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");
        }

        [Fact]
        public async Task SqlBulkTools_BulkDeleteAsync()
        {
            const int rows = 500;

            _bookCollection = _randomizer.GetRandomCollection(rows);
            await BulkDeleteAsync(_dataAccess.GetBookList());

            var results = new List<long>();

            Trace.WriteLine("Testing BulkDeleteAsync with " + rows + " rows");

            for (var i = 0; i < _repeatTimes; i++)
            {
                await BulkInsertAsync(_bookCollection);
                var time = await BulkDeleteAsync(_bookCollection);
                results.Add(time);
                Assert.Equal(0, _dataAccess.GetBookCount());
            }
            var avg = results.Average(l => l);
            Trace.WriteLine("Average result (" + _repeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");
        }

        [Fact]
        public async Task SqlBulkTools_BulkInsertOrUpdateAsync_TestIdentityOutput()
        {
            await BulkDeleteAsync(_dataAccess.GetBookList());
            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.Equal(expected.Id, test.Id);
        }

        [Fact]
        public async Task SqlBulkTools_BulkInsertOrUpdateAsyncWithSelectedColumns_TestIdentityOutput()
        {
            await BulkDeleteAsync(_dataAccess.GetBookList());

            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.Equal(expected.Id, test.Id);
        }

        [Fact]
        public async Task SqlBulkTools_BulkInsertAsync_TestIdentityOutput()
        {
            await BulkDeleteAsync(_dataAccess.GetBookList());

            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(_randomizer.GetRandomCollection(60))
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(80); // Random between random items before test and total items after test. 
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.Equal(expected.Id, test.Id);
        }

        [Fact]
        public async Task SqlBulkTools_BulkInsertAsyncWithSelectedColumns_TestIdentityOutput()
        {

            await BulkDeleteAsync(_dataAccess.GetBookList());

            var books = _randomizer.GetRandomCollection(30);

            var bulk = new BulkOperations();

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            BatchSize = 5000
                        })
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.PublishDate)
                        .BulkInsert()
                        .TmpDisableAllNonClusteredIndexes()
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(15); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.Equal(expected.Id, test.Id);
        }

        [Fact]
        public async Task SqlBulkTools_BulkUpdateAsyncWithSelectedColumns_TestIdentityOutput()
        {
            await BulkDeleteAsync(_dataAccess.GetBookList());

            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);
            await BulkInsertAsync(books);

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.Equal(expected.Id, test.Id);
        }

        [Fact]
        public async Task SqlBulkTools_BulkInsertAsyncWithoutSetter_ThrowsMeaningfulException()
        {
            await BulkDeleteAsync(_dataAccess.GetBookList());

            var bulk = new BulkOperations();

            _bookCollection = _randomizer.GetRandomCollection(30);

            using var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

            using (var conn = new SqlConnection(_dataAccess.ConnectionString))
            {
                await Assert.ThrowsAsync<SqlBulkToolsException>(() =>
                      bulk.Setup()
                          .ForCollection(
                              _bookCollection.Select(
                                  x => new { x.Description, x.ISBN, x.Id, x.Price }))
                          .WithTable("Books")
                          .AddColumn(x => x.Id)
                          .AddColumn(x => x.Description)
                          .AddColumn(x => x.ISBN)
                          .AddColumn(x => x.Price)
                          .BulkInsert()
                          .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                          .CommitAsync(conn));
            }

            trans.Complete();
        }

        [Fact]
        public async Task SqlBulkTools_BulkInsertOrUpdateAsyncWithPrivateIdentityField_ThrowsMeaningfulException()
        {
            await BulkDeleteAsync(_dataAccess.GetBookList());

            var bulk = new BulkOperations();

            var books = _randomizer.GetRandomCollection(30);
            var booksWithPrivateIdentity = new List<BookWithPrivateIdentity>();

            books.ForEach(x => booksWithPrivateIdentity.Add(new BookWithPrivateIdentity()
            {
                ISBN = x.ISBN,
                Description = x.Description,
                Price = x.Price

            }));

            using var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

            using (var conn = new SqlConnection(_dataAccess.ConnectionString))
            {
                await Assert.ThrowsAsync<SqlBulkToolsException>(() =>
                    bulk.Setup<BookWithPrivateIdentity>()
                            .ForCollection(booksWithPrivateIdentity)
                            .WithTable("Books")
                            .AddColumn(x => x.Id)
                            .AddColumn(x => x.Description)
                            .AddColumn(x => x.ISBN)
                            .AddColumn(x => x.Price)
                            .BulkInsertOrUpdate()
                            .MatchTargetOn(x => x.ISBN)
                            .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                            .CommitAsync(conn));
            }

            trans.Complete();

        }

        private async Task<long> BulkInsertAsync(IEnumerable<Book> col)
        {
            var bulk = new BulkOperations();

            var watch = System.Diagnostics.Stopwatch.StartNew();
            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(col)
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            SqlBulkCopyOptions = SqlBulkCopyOptions.TableLock,
                            BatchSize = 3000
                        })
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.PublishDate)
                        .BulkInsert()
                        .CommitAsync(conn);
                }

                trans.Complete();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            return elapsedMs;
        }

        private async Task<long> BulkInsertOrUpdateAsync(IEnumerable<Book> col)
        {
            var bulk = new BulkOperations();

            var watch = System.Diagnostics.Stopwatch.StartNew();
            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(col)
                        .WithTable("Books")
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.PublishDate)
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            return elapsedMs;
        }

        private async Task<long> BulkUpdateAsync(IEnumerable<Book> col)
        {
            var bulk = new BulkOperations();

            var watch = System.Diagnostics.Stopwatch.StartNew();
            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(col)
                        .WithTable("Books")
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.PublishDate)
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            return elapsedMs;
        }

        private async Task<long> BulkDeleteAsync(IEnumerable<Book> col)
        {
            var bulk = new BulkOperations();

            var watch = System.Diagnostics.Stopwatch.StartNew();

            using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var conn = new SqlConnection(_dataAccess.ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(col)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ISBN)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            return elapsedMs;
        }
    }
}
