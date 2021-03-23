using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using NSubstitute;
using SqlBulkTools.TestCommon.Model;
using Xunit;

namespace SqlBulkTools.UnitTests
{
    public class BulkOperationsTests
    {

        [Fact]
        public void GetTableAndSchema_WhenNoSchemaIsSpecified()
        {
            const string expectedSchema = "dbo";
            const string expectedTableName = "MyTable";

            var result = BulkOperationsHelper.GetTableAndSchema("MyTable");

            Assert.Equal(result.Name, expectedTableName);
            Assert.Equal(result.Schema, expectedSchema);
        }

        [Fact]
        public void GetTableAndSchema_WhenASchemaIsSpecified_WithNoFormatting()
        {
            const string expectedSchema = "TestSchema";
            const string expectedTableName = "MyTable";

            var result = BulkOperationsHelper.GetTableAndSchema("TestSchema.MyTable");

            Assert.Equal(result.Name, expectedTableName);
            Assert.Equal(result.Schema, expectedSchema);
        }

        [Fact]
        public void GetTableAndSchema_WhenASchemaIsSpecified_WithFormatting1()
        {
            const string expectedSchema = "TestSchema";
            const string expectedTableName = "MyTable";

            var result = BulkOperationsHelper.GetTableAndSchema("[TestSchema].[MyTable]");

            Assert.Equal(result.Name, expectedTableName);
            Assert.Equal(result.Schema, expectedSchema);
        }

        [Fact]
        public void GetTableAndSchema_WhenASchemaIsSpecified_WithFormatting2()
        {
            var expectedSchema = "TestSchema";
            var expectedTableName = "MyTable";

            var result = BulkOperationsHelper.GetTableAndSchema("[TestSchema].MyTable");

            Assert.Equal(result.Name, expectedTableName);
            Assert.Equal(result.Schema, expectedSchema);
        }

        [Fact]
        public void GetTableAndSchema_WhenASchemaIsSpecified_WithFormatting3()
        {
            var expectedSchema = "TestSchema";
            var expectedTableName = "MyTable";

            var result = BulkOperationsHelper.GetTableAndSchema("TestSchema.[MyTable]");

            Assert.Equal(result.Name, expectedTableName);
            Assert.Equal(result.Schema, expectedSchema);
        }

        [Fact]
        public void GetTableAndSchema_WithAnInvalidName()
        {
            var ex = Assert.Throws<SqlBulkToolsException>(() => 
                BulkOperationsHelper.GetTableAndSchema("TestSchema.InvalidName.MyTable"));
            
            Assert.Equal(ex.Message, "Table name can't contain more than one period '.' character.");
            
        }

        [Fact]
        public void BulkOperationsHelpers_BuildJoinConditionsForUpdateOrInsertWithThreeConditions()
        {
            // Arrange
            var joinOnList = new List<string> { "MarketPlaceId", "FK_BusinessId", "AddressId" };

            // Act
            var result = BulkOperationsHelper.BuildJoinConditionsForInsertOrUpdate(joinOnList.ToArray(), "Source", "Target", new Dictionary<string, string>(), new Dictionary<string, bool>());

            // Assert
            Assert.Equal("ON ([Target].[MarketPlaceId] = [Source].[MarketPlaceId]) AND ([Target].[FK_BusinessId] = [Source].[FK_BusinessId]) AND ([Target].[AddressId] = [Source].[AddressId]) ", result);
        }

        [Fact]
        public void BulkOperationsHelpers_BuildJoinConditionsForUpdateOrInsertWithTwoConditions()
        {
            // Arrange
            var joinOnList = new List<string> { "MarketPlaceId", "FK_BusinessId" };

            // Act
            var result = BulkOperationsHelper.BuildJoinConditionsForInsertOrUpdate(joinOnList.ToArray(), "Source", "Target", new Dictionary<string, string> { { "FK_BusinessId", "DEFAULT_COLLATION" } }, new Dictionary<string, bool>());

            // Assert
            Assert.Equal("ON ([Target].[MarketPlaceId] = [Source].[MarketPlaceId]) AND ([Target].[FK_BusinessId] = [Source].[FK_BusinessId] COLLATE DEFAULT_COLLATION) ", result);
        }

        [Fact]
        public void BulkOperationsHelpers_BuildJoinConditionsForUpdateOrInsertWitSingleCondition()
        {
            // Arrange
            var joinOnList = new List<string> { "MarketPlaceId" };

            // Act
            var result = BulkOperationsHelper.BuildJoinConditionsForInsertOrUpdate(joinOnList.ToArray(), "Source", "Target", new Dictionary<string, string>(), new Dictionary<string, bool>());

            // Assert
            Assert.Equal("ON ([Target].[MarketPlaceId] = [Source].[MarketPlaceId]) ", result);
        }

        [Fact]
        public void BulkOperationsHelpers_BuildUpdateSet_BuildsCorrectSequenceForMultipleColumns()
        {
            // Arrange
            var updateOrInsertColumns = GetTestColumns();
            var expected =
                "SET [Target].[Email] = [Source].[Email], [Target].[id] = [Source].[id], [Target].[IsCool] = [Source].[IsCool], [Target].[Name] = [Source].[Name], [Target].[Town] = [Source].[Town] ";

            // Act
            var result = BulkOperationsHelper.BuildUpdateSet(updateOrInsertColumns, "Source", "Target", null);

            // Assert
            Assert.Equal(expected, result);

        }

        [Fact]
        public void BulkOperationsHelpers_BuildUpdateSet_BuildsCorrectSequenceForSingleColumn()
        {
            // Arrange
            var updateOrInsertColumns = new HashSet<string>();
            updateOrInsertColumns.Add("Id");

            var expected =
                "SET [Target].[Id] = [Source].[Id] ";

            // Act
            var result = BulkOperationsHelper.BuildUpdateSet(updateOrInsertColumns, "Source", "Target", null);

            // Assert
            Assert.Equal(expected, result);

        }

        [Fact]
        public void BulkOperationsHelpers_BuildInsertSet_BuildsCorrectSequenceForMultipleColumns()
        {
            // Arrange
            var updateOrInsertColumns = GetTestColumns();
            var expected =
                "INSERT ([Email], [IsCool], [Name], [Town]) values ([Source].[Email], [Source].[IsCool], [Source].[Name], [Source].[Town])";

            // Act
            var result = BulkOperationsHelper.BuildInsertSet(updateOrInsertColumns, "Source", "id");

            // Assert
            Assert.Equal(expected, result);

        }

        [Fact]
        public void BulkOperationsHelpers_BuildInsertIntoSet_BuildsCorrectSequenceForSingleColumn()
        {
            // Arrange
            var columns = new HashSet<string>();
            columns.Add("Id");
            var tableName = "TableName";

            var expected = "INSERT INTO TableName ([Id]) ";

            // Act
            var result = BulkOperationsHelper.BuildInsertIntoSet(columns, null, tableName);

            // Assert
            Assert.Equal(result, expected);
        }

        [Fact]
        public void BulkOperationsHelpers_BuildInsertIntoSet_BuildsCorrectSequenceForMultipleColumns()
        {
            var columns = GetTestColumns();
            var tableName = "TableName";
            var expected =
                "INSERT INTO TableName ([Email], [IsCool], [Name], [Town]) ";

            // Act
            var result = BulkOperationsHelper.BuildInsertIntoSet(columns, "id", tableName);

            // Assert
            Assert.Equal(result, expected);

        }

        [Fact]
        public void BulkOperationsHelpers_BuildInsertSet_BuildsCorrectSequenceForSingleColumn()
        {
            // Arrange
            var updateOrInsertColumns = new HashSet<string>();
            updateOrInsertColumns.Add("Id");
            var expected =
                "INSERT ([Id]) values ([Source].[Id])";

            // Act
            var result = BulkOperationsHelper.BuildInsertSet(updateOrInsertColumns, "Source", null);

            // Assert
            Assert.Equal(expected, result);

        }

        [Fact]
        public void BulkOperationsHelper_GetAllPropertiesForComplexType_ReturnsCorrectSet()
        {
            // Arrange
            var expected = new HashSet<string> { "AverageEstimate_TotalCost", "AverageEstimate_CreationDate", "Competition", "Id", "MinEstimate_TotalCost", "MinEstimate_CreationDate", "SearchVolume" };
            var propertyInfoList = typeof(ComplexTypeModel).ToPropInfoList();

            // Act
            var result = BulkOperationsHelper.GetAllValueTypeAndStringColumns(propertyInfoList, typeof(ComplexTypeModel));

            // Assert
            Assert.Equal(expected.ToList(), result.ToList());
        }

        [Fact]
        public void BulkOperationsHelper_CreateDataTableForComplexType_IsStructuredCorrectly()
        {
            var columns = new HashSet<string> { "AverageEstimate_TotalCost", "AverageEstimate_CreationDate", "Competition", "MinEstimate_TotalCost", "MinEstimate_CreationDate", "SearchVolume" };
            var propertyInfoList = typeof(ComplexTypeModel).ToPropInfoList();

            var result = BulkOperationsHelper.CreateDataTable<ComplexTypeModel>(propertyInfoList, columns, null, new Dictionary<string, int>());

            Assert.Equal(result.Columns["AverageEstimate_TotalCost"].DataType, typeof(double));
            Assert.Equal(result.Columns["AverageEstimate_CreationDate"].DataType, typeof(DateTime));
            Assert.Equal(result.Columns["MinEstimate_TotalCost"].DataType, typeof(double));
            Assert.Equal(result.Columns["MinEstimate_CreationDate"].DataType, typeof(DateTime));
            Assert.Equal(result.Columns["SearchVolume"].DataType, typeof(double));
            Assert.Equal(result.Columns["Competition"].DataType, typeof(double));
        }

        [Fact]
        public void BulkOperationsHelpers_GetAllValueTypeAndStringColumns_ReturnsCorrectSet()
        {
            // Arrange
            var expected = new HashSet<string> { "BoolTest", "CreatedTime", "IntegerTest", "Price", "Title" };
            var propertyInfoList = typeof(ModelWithMixedTypes).ToPropInfoList();

            // Act
            var result = BulkOperationsHelper.GetAllValueTypeAndStringColumns(propertyInfoList, typeof(ModelWithMixedTypes));

            // Assert
            Assert.Equal(expected.ToList(), result.ToList());
        }
      

        [Fact]
        public void BulkOperationsHelpers_GetIndexManagementCmd_WhenDisableAllIndexesIsTrueReturnsCorrectCmd()
        {
            // Arrange
            const string expected = @"DECLARE @sql AS VARCHAR(MAX)=''; SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' DISABLE;' FROM sys.indexes JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id WHERE sys.indexes.type_desc = 'NONCLUSTERED' AND sys.objects.type_desc = 'USER_TABLE' AND sys.objects.name = '[SqlBulkTools].[dbo].[Books]'; EXEC(@sql);";
            const string databaseName = "SqlBulkTools";

            var sqlConnMock = Substitute.For<IDbConnection>();
            sqlConnMock.Database.Returns(databaseName);

            // Act
            var result = BulkOperationsHelper.GetIndexManagementCmd(Constants.Disable, "Books", "dbo", sqlConnMock);

            // Assert
            Assert.Equal(expected, result);

        }

        [Fact]
        public void BulkOperationsHelpers_RebuildSchema_WithExplicitSchemaIsCorrect()
        {
            // Arrange
            var expected = "[db].[CustomSchemaName].[TableName]";

            // Act
            var result = BulkOperationsHelper.GetFullQualifyingTableName("db", "CustomSchemaName", "TableName");

            // Act
            Assert.Equal(expected, result);
        }

        [Fact]
        public void BulkOperationsHelper_GetDropTmpTableCmd_ReturnsCorrectCmd()
        {
            // Arrange
            var expected = "DROP TABLE #TmpOutput;";

            // Act
            var result = BulkOperationsHelper.GetDropTmpTableCmd();

            // Assert
            Assert.Equal(expected, result);

        }

        [Fact]
        public void BulkOperationsHelper_BuildPredicateQuery_LessThanDecimalCondition()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>
            {
                new PredicateCondition
                {
                    Expression = ExpressionType.LessThan,
                    LeftName = "Price",
                    Value = "50",
                    ValueType = typeof (decimal),
                    SortOrder = 1
                }
            };

            var expected = "AND [Target].[Price] < @PriceCondition1 ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, new Dictionary<string, string>());

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void BulkOperationsHelper_BuildPredicateQuery_IsNullCondition()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>
            {
                new PredicateCondition
                {
                    Expression = ExpressionType.Equal,
                    LeftName = "Description",
                    Value = "null",
                }
            };

            var expected = "AND [Target].[Description] IS NULL ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, null);

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void BulkOperationsHelper_BuildPredicateQuery_IsNotNullCondition()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>
            {
                new PredicateCondition
                {
                    Expression = ExpressionType.NotEqual,
                    LeftName = "Description",
                    Value = "null",
                }
            };

            var expected = "AND [Target].[Description] IS NOT NULL ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, null);

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void BulkOperationsHelper_BuildPredicateQuery_LessThan()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>
            {
                new PredicateCondition
                {
                    Expression = ExpressionType.LessThan,
                    LeftName = "Description",
                    Value = "null",
                    SortOrder = 1
                }
            };

            var expected = "AND [Target].[Description] < @DescriptionCondition1 COLLATE DEFAULT_COLLATION ";

            var hashSet = new HashSet<string> { "DEFAULT_COLLATION" };

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, new Dictionary<string, string> { { "Description", "DEFAULT_COLLATION" } });

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void BulkOperationsHelper_BuildPredicateQuery_LessThanOrEqualTo()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>
            {
                new PredicateCondition
                {
                    Expression = ExpressionType.LessThanOrEqual,
                    LeftName = "Description",
                    Value = "null",
                    SortOrder = 1
                }
            };

            var expected = "AND [Target].[Description] <= @DescriptionCondition1 ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, null);

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void BulkOperationsHelper_BuildPredicateQuery_GreaterThan()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>
            {
                new PredicateCondition
                {
                    Expression = ExpressionType.GreaterThan,
                    LeftName = "Description",
                    Value = "null",
                    SortOrder = 1
                }
            };

            var expected = "AND [Target].[Description] > @DescriptionCondition1 ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, null);

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void BulkOperationsHelper_BuildPredicateQuery_GreaterThanOrEqualTo()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>
            {
                new PredicateCondition
                {
                    Expression = ExpressionType.GreaterThanOrEqual,
                    LeftName = "Description",
                    Value = "null",
                    SortOrder = 1
                }
            };

            var expected = "AND [Target].[Description] >= @DescriptionCondition1 ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, null);

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void BulkOperationsHelper_BuildPredicateQuery_CustomColumnMapping()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>
            {
                new PredicateCondition
                {
                    Expression = ExpressionType.GreaterThanOrEqual,
                    LeftName = "Description",
                    Value = "null",
                    CustomColumnMapping = "ShortDescription",
                    SortOrder = 1
                }
            };

            var expected = "AND [Target].[ShortDescription] >= @DescriptionCondition1 ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, null);

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void BulkOperationsHelper_BuildPredicateQuery_MultipleConditions()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>
            {
                new PredicateCondition
                {
                    Expression = ExpressionType.NotEqual,
                    LeftName = "Description",
                    Value = "null",
                    SortOrder = 1
                },
                new PredicateCondition
                {
                    Expression = ExpressionType.GreaterThanOrEqual,
                    LeftName = "Price",
                    Value = "50",
                    ValueType = typeof(decimal),
                    SortOrder = 2
                },
            };

            var expected = "AND [Target].[Description] IS NOT NULL AND [Target].[Price] >= @PriceCondition2 ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, null);

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void BulkOperationsHelper_BuildPredicateQuery_ThrowsWhenUpdateOnColIsEmpty()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn1 = new string[0];

            var conditions = new List<PredicateCondition>
            {
                new PredicateCondition
                {
                    Expression = ExpressionType.NotEqual,
                    LeftName = "Description",
                    Value = "null",
                    SortOrder = 1
                },
                new PredicateCondition
                {
                    Expression = ExpressionType.GreaterThanOrEqual,
                    LeftName = "Price",
                    Value = "50",
                    ValueType = typeof(decimal),
                    SortOrder = 2
                },
            };

            var expectedMessage = "MatchTargetOn is required for AndQuery.";
            var ex1 = Assert.Throws<SqlBulkToolsException>(() => 
                BulkOperationsHelper.BuildPredicateQuery(updateOn1, conditions, targetAlias, null));

            var ex2 = Assert.Throws<SqlBulkToolsException>(() =>
                BulkOperationsHelper.BuildPredicateQuery(null, conditions, targetAlias, null));
            
            Assert.Equal(ex1.Message,expectedMessage);
            Assert.Equal(ex2.Message,expectedMessage);

        }

        [Fact]
        public void BulkOperationsHelper_BuildValueSet_WithOneValue()
        {
            // Arrange
            var columns = new HashSet<string>();
            columns.Add("TestColumn");

            // Act
            var result = BulkOperationsHelper.BuildValueSet(columns, "Id");

            // Assert
            Assert.Equal("(@TestColumn)", result);
        }

        [Fact]
        public void BulkOperationsHelper_BuildValueSet_WithMultipleValues()
        {
            // Arrange
            var columns = new HashSet<string>();
            columns.Add("TestColumnA");
            columns.Add("TestColumnB");

            // Act
            var result = BulkOperationsHelper.BuildValueSet(columns, "Id");

            // Assert
            Assert.Equal("(@TestColumnA, @TestColumnB)", result);
        }

        [Fact]
        public void BulkOperationsHelper_BuildValueSet_WithMultipleValuesWhenIdentitySet()
        {
            // Arrange
            var columns = new HashSet<string>();
            columns.Add("TestColumnA");
            columns.Add("TestColumnB");
            columns.Add("Id");

            // Act
            var result = BulkOperationsHelper.BuildValueSet(columns, "Id");

            // Assert
            Assert.Equal("(@TestColumnA, @TestColumnB)", result);
        }

        [Fact]
        public void BulkOperationsHelper_AddSqlParamsForUpdateQuery_GetsTypeAndValue()
        {
            var book = new Book
            {
                ISBN = "Some ISBN",
                Price = 23.99M,
                BestSeller = true
            };

            var columns = new HashSet<string>();
            columns.Add("ISBN");
            columns.Add("Price");
            columns.Add("BestSeller");

            var sqlParams = new List<SqlParameter>();
            var propertyInfoList = typeof(Book).ToPropInfoList();


            BulkOperationsHelper.AddSqlParamsForQuery(propertyInfoList, sqlParams, columns, book);

            Assert.Equal(3, sqlParams.Count);
        }

        [Fact]
        public void BulkOperationsHelper_BuildMatchTargetOnListWithMultipleValues_ReturnsCorrectString()
        {
            // Arrange
            var columns = GetTestColumns();

            // ACt
            var result = BulkOperationsHelper.BuildMatchTargetOnList(columns, null, new Dictionary<string, string>());

            // Assert
            Assert.Equal(result, "WHERE [id] = @id AND [Name] = @Name AND [Town] = @Town AND [Email] = @Email AND [IsCool] = @IsCool");
        }

        [Fact]
        public void BulkOperationsHelper_BuildMatchTargetOnListWithSingleValue_ReturnsCorrectString()
        {
            // Arrange
            var columns = new HashSet<string> { "id" };

            // ACt
            var result = BulkOperationsHelper.BuildMatchTargetOnList(columns, new Dictionary<string, string> { { "id", "DEFAULT_COLLATION" } }, new Dictionary<string, string>());

            // Assert
            Assert.Equal(result, "WHERE [id] = @id COLLATE DEFAULT_COLLATION");
        }

        private HashSet<string> GetTestColumns()
        {
            var parameters = new HashSet<string>();

            parameters.Add("id");
            parameters.Add("Name");
            parameters.Add("Town");
            parameters.Add("Email");
            parameters.Add("IsCool");

            return parameters;
        }

        private HashSet<string> GetBookColumns()
        {
            var parameters = new HashSet<string>();

            parameters.Add("Id");
            parameters.Add("ISBN");
            parameters.Add("Title");
            parameters.Add("PublishDate");
            parameters.Add("Price");

            return parameters;
        }
    }
}
