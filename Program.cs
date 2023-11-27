using System.Text;
using Oracle.ManagedDataAccess.Client;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};

app.MapGet("/weatherforecast", () =>
{
    var forecast =  Enumerable.Range(1, 5).Select(index =>
        new WeatherForecast
        (
            DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
            Random.Shared.Next(-20, 55),
            summaries[Random.Shared.Next(summaries.Length)]
        ))
        .ToArray();
    return forecast;
})
.WithName("GetWeatherForecast")
.WithOpenApi();

app.MapGet("/employees", () => 
{
    OracleConnection con = new OracleConnection("User Id=HR;Password=HR;Data Source=localhost:1521/FREEPDB1;");
    con.Open();
    OracleCommand cmd = con.CreateCommand();
    cmd.CommandText = @"
                        SELECT
                            E.EMPLOYEE_ID,
                            E.FIRST_NAME,
                            E.LAST_NAME,
                            E.EMAIL,
                            E.PHONE_NUMBER,
                            E.HIRE_DATE,
                            E.JOB_ID,
                            E.SALARY,
                            E.COMMISSION_PCT,
                            E.MANAGER_ID,
                            E.DEPARTMENT_ID
                        FROM
                            EMPLOYEES E
                    ";
     
    OracleDataReader reader = cmd.ExecuteReader();
    
    List<Employee> employees = new List<Employee>();
    while(reader.Read()) 
    {
        employees.Add(new Employee
        {
            EmployeeId = reader.IsDBNull(0) ? default : reader.GetInt32(0),
            FirstName = reader.IsDBNull(1) ? default : reader.GetString(1),
            LastName = reader.IsDBNull(2) ? default : reader.GetString(2),
            Email = reader.IsDBNull(3) ? default : reader.GetString(3),
            PhoneNumber = reader.IsDBNull(4) ? default : reader.GetString(4),
            HireDate = reader.IsDBNull(5) ? default : reader.GetDateTime(5),
            JobId = reader.IsDBNull(6) ? default : reader.GetString(6),
            Salary = reader.IsDBNull(7) ? default : reader.GetDecimal(7),
            CommissionPct = reader.IsDBNull(8) ? default : reader.GetDecimal(8),
            ManagerId = reader.IsDBNull(9) ? default : reader.GetInt32(9),
            DepartmentId = reader.IsDBNull(10) ? default :reader.GetInt32(10)
        });    
    }

    con.Dispose();
    cmd.Dispose();
    reader.Dispose();

    return employees;
})
.WithName("GetEmployees")
.WithOpenApi();

app.MapGet("/offset-based-employees", (int pageNumber, int pageSize) => 
{
    OracleConnection con = new OracleConnection("User Id=HR;Password=HR;Data Source=localhost:1521/FREEPDB1;");
    con.Open();
    OracleCommand cmd = con.CreateCommand();
    cmd.CommandText = @$"
                        SELECT
                            E.EMPLOYEE_ID,
                            E.FIRST_NAME,
                            E.LAST_NAME,
                            E.EMAIL,
                            E.PHONE_NUMBER,
                            E.HIRE_DATE,
                            E.JOB_ID,
                            E.SALARY,
                            E.COMMISSION_PCT,
                            E.MANAGER_ID,
                            E.DEPARTMENT_ID
                        FROM
                            EMPLOYEES E
                        OFFSET ( {pageNumber} - 1 ) * {pageSize} ROWS FETCH NEXT {pageSize} ROWS ONLY
                    ";
     
    OracleDataReader reader = cmd.ExecuteReader();
    
    List<Employee> employees = new List<Employee>();
    while(reader.Read()) 
    {
        employees.Add(new Employee
        {
            EmployeeId = reader.IsDBNull(0) ? default : reader.GetInt32(0),
            FirstName = reader.IsDBNull(1) ? default : reader.GetString(1),
            LastName = reader.IsDBNull(2) ? default : reader.GetString(2),
            Email = reader.IsDBNull(3) ? default : reader.GetString(3),
            PhoneNumber = reader.IsDBNull(4) ? default : reader.GetString(4),
            HireDate = reader.IsDBNull(5) ? default : reader.GetDateTime(5),
            JobId = reader.IsDBNull(6) ? default : reader.GetString(6),
            Salary = reader.IsDBNull(7) ? default : reader.GetDecimal(7),
            CommissionPct = reader.IsDBNull(8) ? default : reader.GetDecimal(8),
            ManagerId = reader.IsDBNull(9) ? default : reader.GetInt32(9),
            DepartmentId = reader.IsDBNull(10) ? default :reader.GetInt32(10)
        });    
    }

    con.Dispose();
    cmd.Dispose();
    reader.Dispose();

    return employees;
})
.WithName("GetOffsetBasedEmployees")
.WithOpenApi();

app.MapGet("/cursor-based-employees-no-sort", (int pageSize, int? nextToken) => 
{
    OracleConnection con = new OracleConnection("User Id=HR;Password=HR;Data Source=localhost:1521/FREEPDB1;");
    con.Open();
    OracleCommand cmd = con.CreateCommand();
    cmd.CommandText = @$"
                        SELECT
                            E.EMPLOYEE_ID,
                            E.FIRST_NAME,
                            E.LAST_NAME,
                            E.EMAIL,
                            E.PHONE_NUMBER,
                            E.HIRE_DATE,
                            E.JOB_ID,
                            E.SALARY,
                            E.COMMISSION_PCT,
                            E.MANAGER_ID,
                            E.DEPARTMENT_ID
                        FROM
                            EMPLOYEES E
                        WHERE E.EMPLOYEE_ID > {nextToken ?? 0}
                        FETCH NEXT {pageSize} ROWS ONLY
                    ";
     
    OracleDataReader reader = cmd.ExecuteReader();
    
    List<Employee> employees = new List<Employee>();
    while(reader.Read()) 
    {
        employees.Add(new Employee
        {
            EmployeeId = reader.IsDBNull(0) ? default : reader.GetInt32(0),
            FirstName = reader.IsDBNull(1) ? default : reader.GetString(1),
            LastName = reader.IsDBNull(2) ? default : reader.GetString(2),
            Email = reader.IsDBNull(3) ? default : reader.GetString(3),
            PhoneNumber = reader.IsDBNull(4) ? default : reader.GetString(4),
            HireDate = reader.IsDBNull(5) ? default : reader.GetDateTime(5),
            JobId = reader.IsDBNull(6) ? default : reader.GetString(6),
            Salary = reader.IsDBNull(7) ? default : reader.GetDecimal(7),
            CommissionPct = reader.IsDBNull(8) ? default : reader.GetDecimal(8),
            ManagerId = reader.IsDBNull(9) ? default : reader.GetInt32(9),
            DepartmentId = reader.IsDBNull(10) ? default :reader.GetInt32(10)
        });    
    }

    con.Dispose();
    cmd.Dispose();
    reader.Dispose();

    var lastEmployee = employees.LastOrDefault();
    int? lastEmployeeId = null;
    if(lastEmployee is not null)
    {
        lastEmployeeId = lastEmployee.EmployeeId;
    }

    return new Paginated<Employee, int?>
    {
        Data = employees,
        NextToken = lastEmployeeId
    };
})
.WithName("GetCursorBasedEmployeesNoSort")
.WithOpenApi();

app.MapGet("/cursor-based-employees-with-sorting-any-fields", (int pageSize, string? nextToken, string? tokenFields) => 
{
    OracleConnection con = new OracleConnection("User Id=HR;Password=HR;Data Source=localhost:1521/FREEPDB1;");
    con.Open();
    
    var orderByString = !string.IsNullOrEmpty(tokenFields) ? $"ORDER BY {getFields(tokenFields)}" : null; 
    var conditionString = !string.IsNullOrEmpty(nextToken) ? $"WHERE {getCondition(nextToken, tokenFields)}" : null;

    OracleCommand cmd = con.CreateCommand();
    cmd.CommandText = @$"
                        SELECT
                            E.EMPLOYEE_ID,
                            E.FIRST_NAME,
                            E.LAST_NAME,
                            E.EMAIL,
                            E.PHONE_NUMBER,
                            E.HIRE_DATE,
                            E.JOB_ID,
                            E.SALARY,
                            E.COMMISSION_PCT,
                            E.MANAGER_ID,
                            E.DEPARTMENT_ID
                        FROM
                            EMPLOYEES E
                        {conditionString ?? default}
                        {orderByString ?? default}
                        FETCH NEXT {pageSize} ROWS ONLY
                    ";
     
    OracleDataReader reader = cmd.ExecuteReader();
    
    List<Employee> employees = new List<Employee>();
    while(reader.Read()) 
    {
        employees.Add(new Employee
        {
            EmployeeId = reader.IsDBNull(0) ? default : reader.GetInt32(0),
            FirstName = reader.IsDBNull(1) ? default : reader.GetString(1),
            LastName = reader.IsDBNull(2) ? default : reader.GetString(2),
            Email = reader.IsDBNull(3) ? default : reader.GetString(3),
            PhoneNumber = reader.IsDBNull(4) ? default : reader.GetString(4),
            HireDate = reader.IsDBNull(5) ? default : reader.GetDateTime(5),
            JobId = reader.IsDBNull(6) ? default : reader.GetString(6),
            Salary = reader.IsDBNull(7) ? default : reader.GetDecimal(7),
            CommissionPct = reader.IsDBNull(8) ? default : reader.GetDecimal(8),
            ManagerId = reader.IsDBNull(9) ? default : reader.GetInt32(9),
            DepartmentId = reader.IsDBNull(10) ? default :reader.GetInt32(10)
        });    
    }

    con.Dispose();
    cmd.Dispose();
    reader.Dispose();

    var lastEmployee = employees.LastOrDefault();
    string? lastSeenEmployeeToken = null;
    if(lastEmployee is not null)
    {
        // dinamik olmalı
        lastSeenEmployeeToken = $"{lastEmployee.HireDate},{lastEmployee.Salary},{lastEmployee.EmployeeId}";
    }

    return new Paginated<Employee, string?>
    {
        Data = employees,
        NextToken = lastSeenEmployeeToken
    };
})
.WithName("GetCursorBasedEmployeesWithSortAnyFields")
.WithOpenApi();

string? getFields(string? sortFields)
{
    if(string.IsNullOrEmpty(sortFields))
        return string.Empty;
 
    string[] fields = sortFields.Split(",");

    List<string> orderyByField = new List<string>();
    foreach(var field in fields)
    {   
        switch (field)
        {
            case "employeeId":
                orderyByField.Add("EMPLOYEE_ID");
                break;
            case "hireDate":
                orderyByField.Add("HIRE_DATE");
                break;
            case "salary":
                orderyByField.Add("SALARY");
                break;
            default:
                break;
        }
    }
    return String.Join(',', orderyByField);
}

string? getCondition(string? nextTokenFields, string? sortFields)
{
    if(string.IsNullOrEmpty(sortFields) || string.IsNullOrEmpty(nextTokenFields))
        return string.Empty;

    string[] tokens = nextTokenFields.Split(",");
    string[] fields = getFields(sortFields).Split(",");

    if(tokens.Length != fields.Length)
        return string.Empty;
    
    string condition = "";
    for(int i = 0; i < tokens.Length; i++)
    {
        if(i > 0) {
        condition += " or ";
        }
        condition += "(";
        for(int j = 0; j <= i; j++)
        {
            if(i == j)
            {
                condition += $"{fields[j]} > {tokens[j]})";
            } else {
                condition += $"{fields[j]} = {tokens[j]} and ";
            }
        }
    }

    return condition;
}

app.Run();

record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}
