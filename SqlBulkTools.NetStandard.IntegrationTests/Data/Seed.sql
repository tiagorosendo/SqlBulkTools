-- CREATE DATABASE IF NOT EXISTS

IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'SqlBulkTools')
			BEGIN
				CREATE DATABASE [SqlBulkTools]
			END
GO

USE [SqlBulkTools]
GO

-- DROP STORED PROCEDURES IF EXISTS
IF EXISTS (
		SELECT * 
		FROM INFORMATION_SCHEMA.ROUTINES
		WHERE SPECIFIC_CATALOG = 'SqlBulkTools'
		AND SPECIFIC_NAME = 'GetBooks'
		)
		BEGIN
			DROP PROCEDURE dbo.GetBooks
		END

GO

IF EXISTS (
		SELECT * 
		FROM INFORMATION_SCHEMA.ROUTINES
		WHERE SPECIFIC_CATALOG = 'SqlBulkTools'
		AND SPECIFIC_NAME = 'GetCustomIdentityColumnNameTestList'
		)
		BEGIN
			DROP PROCEDURE dbo.GetCustomIdentityColumnNameTestList
		END

GO

IF EXISTS (
		SELECT * 
		FROM INFORMATION_SCHEMA.ROUTINES
		WHERE SPECIFIC_CATALOG = 'SqlBulkTools'
		AND SPECIFIC_NAME = 'GetComplexModelCount'
		)
		BEGIN
			DROP PROCEDURE dbo.GetComplexModelCount
		END

GO

IF EXISTS (
		SELECT * 
		FROM INFORMATION_SCHEMA.ROUTINES
		WHERE SPECIFIC_CATALOG = 'SqlBulkTools'
		AND SPECIFIC_NAME = 'GetComplexModelList'
		)
		BEGIN
			DROP PROCEDURE dbo.GetComplexModelList
		END

GO

IF EXISTS (
		SELECT * 
		FROM INFORMATION_SCHEMA.ROUTINES
		WHERE SPECIFIC_CATALOG = 'SqlBulkTools'
		AND SPECIFIC_NAME = 'GetBookCount'
		)
		BEGIN
			DROP PROCEDURE dbo.GetBookCount
		END

GO

IF EXISTS (
		SELECT * 
		FROM INFORMATION_SCHEMA.ROUTINES
		WHERE SPECIFIC_CATALOG = 'SqlBulkTools'
		AND SPECIFIC_NAME = 'GetSchemaTest'
		)
		BEGIN
			DROP PROCEDURE dbo.GetSchemaTest
		END

GO

IF EXISTS (
		SELECT * 
		FROM INFORMATION_SCHEMA.ROUTINES
		WHERE SPECIFIC_CATALOG = 'SqlBulkTools'
		AND SPECIFIC_NAME = 'GetCustomColumnMappingTests'
		)
		BEGIN
			DROP PROCEDURE dbo.GetCustomColumnMappingTests
		END

GO

IF EXISTS (
		SELECT * 
		FROM INFORMATION_SCHEMA.ROUTINES
		WHERE SPECIFIC_CATALOG = 'SqlBulkTools'
		AND SPECIFIC_NAME = 'GetReservedColumnNameTests'
		)
		BEGIN
			DROP PROCEDURE dbo.GetReservedColumnNameTests
		END

GO

IF EXISTS (
		SELECT * 
		FROM INFORMATION_SCHEMA.ROUTINES
		WHERE SPECIFIC_CATALOG = 'SqlBulkTools'
		AND SPECIFIC_NAME = 'ReseedBookIdentity'
		)
		BEGIN
			DROP PROCEDURE dbo.ReseedBookIdentity
		END

GO

-- DROP TABLES IF EXISTS

IF EXISTS(
		SELECT * 
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_CATALOG = 'SqlBulkTools'
		AND TABLE_NAME = 'Books'
		)
		BEGIN
			DROP TABLE dbo.Books
		END

IF EXISTS(
		SELECT * 
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_CATALOG = 'SqlBulkTools'
		AND TABLE_NAME = 'CustomIdentityColumnNameTest'
		)
		BEGIN
			DROP TABLE dbo.CustomIdentityColumnNameTest
		END

IF EXISTS(
		SELECT * 
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_CATALOG = 'SqlBulkTools'
		AND TABLE_NAME = 'ComplexTypeTest'
		)
		BEGIN
			DROP TABLE dbo.ComplexTypeTest
		END

IF EXISTS(
		SELECT *
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_CATALOG = 'SqlBulkTools'
		AND TABLE_NAME = 'SchemaTest'
		AND TABLE_SCHEMA = 'AnotherSchema'
		)
		BEGIN
			DROP TABLE AnotherSchema.SchemaTest
		END

IF EXISTS(
		SELECT *
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_CATALOG = 'SqlBulkTools'
		AND TABLE_NAME = 'SchemaTest'
		AND TABLE_SCHEMA = 'dbo'
		)
		BEGIN
			DROP TABLE dbo.SchemaTest
		END

IF EXISTS(
		SELECT * 
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_CATALOG = 'SqlBulkTools'
		AND TABLE_NAME = 'CustomColumnMappingTests'
		)
		BEGIN
			DROP TABLE dbo.CustomColumnMappingTests
		END

IF EXISTS(
		SELECT * 
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_CATALOG = 'SqlBulkTools'
		AND TABLE_NAME = 'ReservedColumnNameTests'
		)
		BEGIN
			DROP TABLE dbo.ReservedColumnNameTests
		END

IF EXISTS(
		SELECT * 
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_CATALOG = 'SqlBulkTools'
		AND TABLE_NAME = 'TestDataTypes'
		)
		BEGIN
			DROP TABLE dbo.TestDataTypes
		END

-- DROP SCHEMAS IF EXISTS

IF EXISTS(SELECT *
		FROM INFORMATION_SCHEMA.SCHEMATA
		WHERE CATALOG_NAME = 'SqlBulkTools'
		AND SCHEMA_NAME = 'AnotherSchema'
		)
		BEGIN
			DROP SCHEMA AnotherSchema
		END

GO

-- CREATE SCHEMAS

CREATE SCHEMA AnotherSchema

GO

-- CREATE TABLES

CREATE TABLE [dbo].[CustomIdentityColumnNameTest] 
(
	ID_COMPANY INT IDENTITY(1,1) NOT NULL,
	ColumnA nvarchar(256) NOT NULL
)

CREATE TABLE [dbo].[Books](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[WarehouseId] [int] NULL,
	[ISBN] [nvarchar](13) NULL,
	[Title] [nvarchar](256) NULL,
	[Description] [nvarchar](2000) NULL,
	[PublishDate] [datetime] NULL,
	[Price] [decimal](18, 2) NOT NULL,
	[TestFloat] [real] NULL,
	[TestNullableInt] [int] NULL,
	[BestSeller] [bit] NULL,
	[CreatedAt] [datetime] NULL,
	[ModifiedAt] [datetime] NULL,
 CONSTRAINT [PK_dbo.Books] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE TABLE [dbo].[ComplexTypeTest](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[MinEstimate_TotalCost] float NOT NULL,
	[MinEstimate_CreationDate] DateTime NOT NULL,
	[AverageEstimate_TotalCost] float NOT NULL,
	[AverageEstimate_CreationDate] DateTime NOT NULL,
	[SearchVolume] float NOT NULL,
	[Competition] float NOT NULL,
 CONSTRAINT [PK_dbo.ComplexTypeTest] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]



CREATE TABLE [AnotherSchema].[SchemaTest](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[ColumnA] [nvarchar](max) NULL,
 CONSTRAINT [PK_AnotherSchema.SchemaTest] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

CREATE TABLE [dbo].[CustomColumnMappingTests](
	[NaturalId] [int] NOT NULL,
	[ColumnX] [nvarchar](256) NULL,
	[ColumnY] [int] NOT NULL,
 CONSTRAINT [PK_dbo.CustomColumnMappingTests] PRIMARY KEY CLUSTERED 
(
	[NaturalId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE TABLE [dbo].[ReservedColumnNameTests](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Key] [int] NOT NULL,
 CONSTRAINT [PK_dbo.ReservedColumnNameTests] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE TABLE [dbo].[SchemaTest](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[ColumnB] [nvarchar](max) NULL,
 CONSTRAINT [PK_dbo.SchemaTest] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

CREATE TABLE [dbo].[TestDataTypes](
	[FloatTest] [real] NULL,
	[FloatTest2] [float] NULL,
	[DecimalTest] [decimal](14, 2) NULL,
	[MoneyTest] [money] NULL,
	[SmallMoneyTest] [smallmoney] NULL,
	[NumericTest] [numeric](30, 7) NULL,
	[RealTest] [real] NULL,
	[DateTimeTest] [datetime] NULL,
	[DateTime2Test] [datetime2](7) NULL,
	[SmallDateTimeTest] [smalldatetime] NULL,
	[DateTest] [date] NULL,
	[TimeTest] [time](7) NULL,
	[GuidTest] [uniqueidentifier] NULL,
	[TextTest] [text] NULL,
	[VarBinaryTest] [varbinary](20) NULL,
	[BinaryTest] [binary](6) NULL,
	[TinyIntTest] [tinyint] NULL,
	[BigIntTest] [bigint] NULL,
	[CharTest] [char](17) NULL,
	[ImageTest] [image] NULL,
	[NTextTest] [ntext] NULL,
	[NCharTest] [nchar](10) NULL,
	[XmlTest] [xml] NULL,
	[TestSqlGeometry] [geometry] NULL,
	[TestSqlGeography] [geography] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

-- CREATE STORED PROCEDURES

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE GetBooks
@Isbn nvarchar(13) = null
AS
BEGIN
	SET NOCOUNT ON;
	SELECT * 
	FROM dbo.Books
	WHERE @Isbn IS NULL OR ISBN = @Isbn
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE GetBookCount
AS
BEGIN
	SET NOCOUNT ON;

    SELECT COUNT(*) as BookCount
	FROM dbo.Books
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE GetSchemaTest
	@Schema nvarchar(64)
AS
BEGIN
	SET NOCOUNT ON;

    IF (@Schema = 'dbo')
		BEGIN
			SELECT *
			FROM [dbo].SchemaTest
		END
	ELSE IF (@Schema = 'AnotherSchema') 
		BEGIN
			SELECT *
			FROM [AnotherSchema].SchemaTest
		END
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE GetCustomColumnMappingTests
AS
BEGIN
	SET NOCOUNT ON;

    SELECT *
	FROM CustomColumnMappingTests
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE GetReservedColumnNameTests
AS
BEGIN
	SET NOCOUNT ON;

	SELECT *
	FROM dbo.ReservedColumnNameTests
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE ReseedBookIdentity
	@IdStart int
AS
BEGIN
	DBCC CHECKIDENT ('[dbo].[Books]', RESEED, @IdStart);
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE GetComplexModelCount
AS
BEGIN
	SET NOCOUNT ON;

    SELECT COUNT(*)
	FROM ComplexTypeTest
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE GetCustomIdentityColumnNameTestList
AS
BEGIN
	SET NOCOUNT ON;

	SELECT ID_COMPANY, ColumnA
	FROM dbo.CustomIdentityColumnNameTest
END
GO

