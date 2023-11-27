public class Employee
{
    public int EmployeeId { get; set; }
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string? Email { get; set; }
    public string? PhoneNumber { get; set; }
    public DateTime HireDate { get; set; }
    public string? JobId { get; set; }
    public decimal Salary { get; set; }
    public decimal CommissionPct { get; set; }
    public int ManagerId { get; set; }
    public int DepartmentId { get; set; }
}

public class Paginated<T, Y>
{
    public List<T> Data { get; set; }
    public Y? NextToken { get; set; }
}