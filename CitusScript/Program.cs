//from low to high priority
List<string> rankedColumns = ["id", "auto_id", "user_id", "uid", "gid", "categoryid", "objectid", "collection_id", "circle_id", "appid", "token", "app"];

string inputFilePath = "input.txt";
char rowSeparator = '\n';
char columnSeparator = '|';


try
{
    var lines = File.ReadAllText(inputFilePath).Trim().Split(rowSeparator);

    var header = lines[0].Split(columnSeparator).Select(x => x.Trim()).ToList();
    var tableNameIndex = FindHeaderIndex("table_name");
    var columnNameIndex = FindHeaderIndex("column_name");
    var dataTypeIndex = FindHeaderIndex("data_type");
    var nullableIndex = FindHeaderIndex("is_nullable");
    var primaryKeyIndex = FindHeaderIndex("primary_key");
    var foreignTableNameIndex = FindHeaderIndex("foreign_table_name");
    var foreignColumnNameIndex = FindHeaderIndex("foreign_column_name");

    Dictionary<string,TableData> tables = [];

    foreach (var line in lines.Skip(1))
    {
        var segments = line.Split(columnSeparator).Select(x => x.Trim()).ToArray();
        if (segments.Length == 1)
            continue;

        var tableName = FindSegment(tableNameIndex);
        if (!tables.TryGetValue(tableName, out var table))
            table = tables[tableName] = new();

        var columnName = FindSegment(columnNameIndex);
        table.Columns.Add(columnName, new(
            dataType: FindSegment(dataTypeIndex),
            nullable: FindSegment(nullableIndex) switch { "NO" => false, "YES" => true, _ => throw new Exception($"Unrecognized boolean for {tableName}.{columnName}: {FindSegment(nullableIndex)}") },
            primaryKey: FindSegment(primaryKeyIndex) != "" && (FindSegment(primaryKeyIndex) == columnName ? true : throw new Exception($"Unrecognized primary key for {tableName}.{columnName}: {FindSegment(primaryKeyIndex)}")),
            foreignColumn: BuildReference()
        ));



        string FindSegment(int index)
            => segments[index].Trim();



        ColumnReferenceData? BuildReference()
        {
            var foreignTableName = FindSegment(foreignTableNameIndex);
            var foreignColumnName = FindSegment(foreignColumnNameIndex);
            if (foreignTableName == "")
                return foreignColumnName == "" ? null : throw new Exception($"Mixed foreign key reference for {tableName}.{columnName}: table=null and column={foreignColumnName}");
            else return foreignColumnName != "" ? new(foreignTableName, foreignColumnName) : throw new Exception($"Mixed foreign key reference for {tableName}.{columnName}: table={foreignTableName} and column=null");
        }
    }

    List<string> result = [];

    foreach (var tableName in tables.Keys)
        ProcessTable(tableName);

    foreach (var command in result)
        Console.WriteLine(command);



    int FindHeaderIndex(string key)
    {
        var result = header.IndexOf(key);
        if (result < 0)
            throw new Exception($"Column '{key}' not found!");
        return result;
    }

    void ProcessTable(string tableName)
    {
        if (!tables.TryGetValue(tableName, out var table))
            throw new Exception($"Unrecognized table reference: {tableName}");
        switch (table.State)
        {
            case TableState.Generated:
                return;
            case TableState.Processing:
                throw new Exception($"Circular reference detected: {tableName}");
        }

        table.State = TableState.Processing;
        int maxRating = -1;
        List<KeyValuePair<string,ColumnData>> columnOptions = [];
        foreach (var columnKV in table.Columns)
        {
            var columnName = columnKV.Key;
            var columnData = columnKV.Value;

            if (columnData.ForeignColumn != null)
                ProcessTable(columnData.ForeignColumn.TableName);

            if (!columnData.PrimaryKey)
                continue;

            var rating = rankedColumns.IndexOf(columnKV.Key);
            if (rating == maxRating)
                columnOptions.Add(columnKV);
            else if (rating > maxRating)
            {
                maxRating = rating;
                columnOptions = [columnKV];
            }
        }

        if (columnOptions.Count == 0)
            throw new Exception($"No primary keys for table: {tableName}");
        if (columnOptions.Count > 1)
            throw new Exception($"Not enough ranking in table {tableName}: {string.Join(", ", columnOptions.Select(x => $"{x.Value.DataType} {x.Key}"))}");

        result.Add($"SELECT create_distributed_table('{tableName}', '{columnOptions.First().Key}');");
        table.State = TableState.Generated;
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Failed: {ex.Message}");
}



class TableData()
{
    public Dictionary<string,ColumnData> Columns = [];

    public TableState State = TableState.Unseen;
}



class ColumnData(string dataType, bool nullable, bool primaryKey, ColumnReferenceData? foreignColumn)
{
    public string DataType = dataType;

    public bool Nullable = nullable;

    public bool PrimaryKey = primaryKey;

    public ColumnReferenceData? ForeignColumn = foreignColumn;
}



class ColumnReferenceData(string tableName, string columnName)
{
    public string TableName = tableName;

    public string ColumnName = columnName;
}



enum TableState
{
    Unseen,
    Processing,
    Generated
}