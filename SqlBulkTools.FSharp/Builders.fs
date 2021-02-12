module SqlBulkTools.FSharp

open System.Data
open SqlBulkTools
open SqlBulkTools.BulkCopy

type Operation<'T> = 
    | OpNone
    | OpSetup of Setup
    | OpForCollection of BulkForCollection<'T>
    | OpWithTable of BulkTable<'T>
    | OpAddColumn of BulkAddColumn<'T>
    | OpUpdate of BulkUpdate<'T>
    | OpUpsert of BulkInsertOrUpdate<'T>
    | OpDelete of BulkDelete<'T>

type BulkInsertBuilder(conn: IDbConnection) = 
    let def = OpNone

    member this.For (rows: seq<'T>, f: 'T -> Operation<'T>) =
        OpForCollection (BulkOperations().Setup().ForCollection(rows))

    member this.Yield _ = 
        def

    [<CustomOperation("table", MaintainsVariableSpace = true)>]
    member this.Table (props, tbl) = 
        match props with
        | OpForCollection bulk -> OpWithTable(bulk.WithTable tbl)
        | _ -> failwith "Must add collection first."

    [<CustomOperation("column", MaintainsVariableSpace=true)>]
    member this.Column (props, [<ProjectionParameter>] colExpr) =
        match props with
        | OpWithTable bulk -> OpAddColumn (bulk.AddColumn(colExpr))
        | OpAddColumn bulk -> OpAddColumn (bulk.AddColumn(colExpr))
        | _ -> failwith "Must add table first."

    [<CustomOperation("columnDest", MaintainsVariableSpace=true)>]
    member this.ColumnDestination (props, [<ProjectionParameter>] colExpr, destination: string) =
        match props with
        | OpWithTable bulk -> OpAddColumn (bulk.AddColumn(colExpr, destination))
        | OpAddColumn bulk -> OpAddColumn (bulk.AddColumn(colExpr, destination))
        | _ -> failwith "Must add table first."

    member this.Run (props) =
        match props with
        | OpAddColumn bulk -> bulk.BulkInsert().Commit(conn)
        | _ -> failwith "Must add at least one column first."

/// A bulk insert will attempt to insert all records. If you have any unique constraints on columns, these must be respected. 
/// Notes: (1) Only the columns configured (via AddColumn) will be evaluated.
let bulkInsert conn = BulkInsertBuilder(conn)


type BulkUpdateBuilder(conn: IDbConnection) = 
    let def = OpNone

    member this.For (rows: seq<'T>, f: 'T -> Operation<'T>) =
        OpForCollection (BulkOperations().Setup().ForCollection(rows))

    member this.Yield _ = 
        def

    [<CustomOperation("table", MaintainsVariableSpace = true)>]
    member this.Table (props, tbl) = 
        match props with
        | OpForCollection bulk -> OpWithTable(bulk.WithTable tbl)
        | _ -> failwith "Must add collection first."

    [<CustomOperation("column", MaintainsVariableSpace=true)>]
    member this.Column (props, [<ProjectionParameter>] colExpr) =
        match props with
        | OpWithTable bulk -> OpAddColumn (bulk.AddColumn(colExpr))
        | OpAddColumn bulk -> OpAddColumn (bulk.AddColumn(colExpr))
        | _ -> failwith "Must add table first."

    [<CustomOperation("columnDest", MaintainsVariableSpace=true)>]
    member this.ColumnDestination (props, [<ProjectionParameter>] colExpr, destination: string) =
        match props with
        | OpWithTable bulk -> OpAddColumn (bulk.AddColumn(colExpr, destination))
        | OpAddColumn bulk -> OpAddColumn (bulk.AddColumn(colExpr, destination))
        | _ -> failwith "Must add table first."

    [<CustomOperation("matchTargetOn", MaintainsVariableSpace=true)>]
    member this.MatchTargetOn (props, [<ProjectionParameter>] colExpr) =
        match props with
        | OpAddColumn bulk -> OpUpdate (bulk.BulkUpdate().MatchTargetOn(colExpr))
        | OpUpdate bulk -> OpUpdate (bulk.MatchTargetOn(colExpr))
        | _ -> failwith "Must add columns first."

    [<CustomOperation("updateWhen", MaintainsVariableSpace=true)>]
    member this.UpdateWhen (props, [<ProjectionParameter>] filter) =
        match props with
        | OpAddColumn bulk -> OpUpdate (bulk.BulkUpdate().UpdateWhen(filter))
        | OpUpdate bulk -> OpUpdate (bulk.UpdateWhen(filter))
        | _ -> failwith "Must add columns first."

    member this.Run (props) =
        match props with
        | OpUpdate bulk -> bulk.Commit(conn)
        | _ -> failwith "Must add at least one column first."

/// A bulk update will attempt to update any matching records. Notes: (1) BulkUpdate requires at least one MatchTargetOn 
/// property to be configured. (2) Only the columns configured (via AddColumn) will be evaluated.
let bulkUpdate conn = BulkUpdateBuilder(conn)


type BulkUpsertBuilder(conn: IDbConnection) = 
    let def = OpNone

    member this.For (rows: seq<'T>, f: 'T -> Operation<'T>) =
        OpForCollection (BulkOperations().Setup().ForCollection(rows))

    member this.Yield _ = 
        def

    [<CustomOperation("table", MaintainsVariableSpace = true)>]
    member this.Table (props, tbl) = 
        match props with
        | OpForCollection bulk -> OpWithTable(bulk.WithTable tbl)
        | _ -> failwith "Must add collection first."

    [<CustomOperation("column", MaintainsVariableSpace=true)>]
    member this.Column (props, [<ProjectionParameter>] colExpr) =
        match props with
        | OpWithTable bulk -> OpAddColumn (bulk.AddColumn(colExpr))
        | OpAddColumn bulk -> OpAddColumn (bulk.AddColumn(colExpr))
        | _ -> failwith "Must add table first."

    [<CustomOperation("columnDest", MaintainsVariableSpace=true)>]
    member this.ColumnDestination (props, [<ProjectionParameter>] colExpr, destination: string) =
        match props with
        | OpWithTable bulk -> OpAddColumn (bulk.AddColumn(colExpr, destination))
        | OpAddColumn bulk -> OpAddColumn (bulk.AddColumn(colExpr, destination))
        | _ -> failwith "Must add table first."

    [<CustomOperation("matchTargetOn", MaintainsVariableSpace=true)>]
    member this.MatchTargetOn (props, [<ProjectionParameter>] colExpr) =
        match props with
        | OpAddColumn bulk -> OpUpsert (bulk.BulkInsertOrUpdate().MatchTargetOn(colExpr))
        | OpUpsert bulk -> OpUpsert (bulk.MatchTargetOn(colExpr))
        | _ -> failwith "Must add columns first."

    [<CustomOperation("updateWhen", MaintainsVariableSpace=true)>]
    member this.UpdateWhen (props, [<ProjectionParameter>] filter) =
        match props with
        | OpAddColumn bulk -> OpUpsert (bulk.BulkInsertOrUpdate().UpdateWhen(filter))
        | OpUpsert bulk -> OpUpsert (bulk.UpdateWhen(filter))
        | _ -> failwith "Must add columns first."

    member this.Run (props) =
        match props with
        | OpUpsert bulk -> bulk.Commit(conn)
        | _ -> failwith "Must add at least one column first."

/// A bulk insert or update is also known as bulk upsert or merge. All matching rows from the source will be updated.
/// Any unique rows not found in target but exist in source will be added. Notes: (1) BulkInsertOrUpdate requires at least 
/// one MatchTargetOn property to be configured. (2) Only the columns configured (via AddColumn) 
/// will be evaluated.
let bulkUpsert conn = BulkUpsertBuilder(conn)


type BulkDeleteBuilder(conn: IDbConnection) = 
    let def = OpNone

    member this.For (rows: seq<'T>, f: 'T -> Operation<'T>) =
        OpForCollection (BulkOperations().Setup().ForCollection(rows))

    member this.Yield _ = 
        def

    [<CustomOperation("table", MaintainsVariableSpace = true)>]
    member this.Table (props, tbl) = 
        match props with
        | OpForCollection bulk -> OpWithTable(bulk.WithTable tbl)
        | _ -> failwith "Must add collection first."

    [<CustomOperation("column", MaintainsVariableSpace=true)>]
    member this.Column (props, [<ProjectionParameter>] colExpr) =
        match props with
        | OpWithTable bulk -> OpAddColumn (bulk.AddColumn(colExpr))
        | OpAddColumn bulk -> OpAddColumn (bulk.AddColumn(colExpr))
        | _ -> failwith "Must add table first."

    [<CustomOperation("columnDest", MaintainsVariableSpace=true)>]
    member this.ColumnDestination (props, [<ProjectionParameter>] colExpr, destination: string) =
        match props with
        | OpWithTable bulk -> OpAddColumn (bulk.AddColumn(colExpr, destination))
        | OpAddColumn bulk -> OpAddColumn (bulk.AddColumn(colExpr, destination))
        | _ -> failwith "Must add table first."

    [<CustomOperation("matchTargetOn", MaintainsVariableSpace=true)>]
    member this.MatchTargetOn (props, [<ProjectionParameter>] colExpr) =
        match props with
        | OpAddColumn bulk -> OpDelete (bulk.BulkDelete().MatchTargetOn(colExpr))
        | OpDelete bulk -> OpDelete (bulk.MatchTargetOn(colExpr))
        | _ -> failwith "Must add columns first."

    [<CustomOperation("deleteWhen", MaintainsVariableSpace=true)>]
    member this.UpdateWhen (props, [<ProjectionParameter>] filter) =
        match props with
        | OpAddColumn bulk -> OpDelete (bulk.BulkDelete().DeleteWhen(filter))
        | OpDelete bulk -> OpDelete (bulk.DeleteWhen(filter))
        | _ -> failwith "Must add columns first."

    member this.Run (props) =
        match props with
        | OpDelete bulk -> bulk.Commit(conn)
        | _ -> failwith "Must add at least one column first."

/// A bulk delete will delete records when matched. Consider using a DTO with only the needed information (e.g. PK) Notes: 
/// (1) BulkUpdate requires at least one MatchTargetOn property to be configured.
let bulkDelete conn = BulkDeleteBuilder(conn)