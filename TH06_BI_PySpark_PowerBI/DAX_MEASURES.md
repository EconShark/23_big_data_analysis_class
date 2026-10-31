# DAX MEASURES

```DAX
Total Revenue = SUM(FactSales[Revenue])

Total Profit = SUM(FactSales[Profit])

Total Cost = SUM(FactSales[Cost])

Total Orders = DISTINCTCOUNT(FactSales[OrderID])

Total Customers = DISTINCTCOUNT(FactSales[CustomerKey])

Units Sold = SUM(FactSales[Quantity])

Average Order Value = DIVIDE([Total Revenue], [Total Orders])

Profit Margin % = DIVIDE([Total Profit], [Total Revenue])

Revenue per Customer = DIVIDE([Total Revenue], [Total Customers])

Return Rate % = DIVIDE(CALCULATE(COUNTROWS(FactSales), FactSales[IsReturn] = 1), COUNTROWS(FactSales))

Revenue LY = CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(DimDate[Date]))

Revenue YoY % = DIVIDE([Total Revenue] - [Revenue LY], [Revenue LY])

Revenue YTD = TOTALYTD([Total Revenue], DimDate[Date])

Profit YTD = TOTALYTD([Total Profit], DimDate[Date])

Running Revenue =
CALCULATE(
    [Total Revenue],
    FILTER(ALLSELECTED(DimDate[Date]), DimDate[Date] <= MAX(DimDate[Date]))
)

Product Revenue Rank = RANKX(ALL(DimProduct[ProductName]), [Total Revenue], , DESC)
```
