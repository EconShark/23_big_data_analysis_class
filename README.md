
**BÀI TẬP TÌNH HUỐNG THỰC HÀNH**

**Môn: Phân tích dữ liệu lớn trong đầu tư và kinh doanh**

Sinh viên thực hiện lần lượt 06 bài tập tình huống. Mỗi bài mô phỏng một công việc thực tế của Data Analyst, từ xử lý dữ liệu bằng Python/Pandas, ứng dụng GenAI, phân tích dữ liệu lớn bằng PySpark đến xây dựng dashboard BI.

**TH01 – Python/Pandas for Business Analytics**

Tình huống: Doanh nghiệp có dữ liệu giao dịch bán hàng nhưng dữ liệu còn thiếu, trùng lặp và có một số giá trị bất thường.

Nhiệm vụ: Sử dụng Python, NumPy và Pandas để:

* kiểm tra và làm sạch dữ liệu;
* tính các KPI như Revenue, Profit, AOV, Profit Margin;
* phân tích theo thời gian, sản phẩm, khu vực, kênh bán hàng và khách hàng;
* trực quan hóa dữ liệu;
* rút ra các nhận xét và đề xuất kinh doanh.

Kết quả: Notebook phân tích và phần kết luận Business Insights.

**TH02 – Ứng dụng Generative AI trong công việc Data Analyst**

Tình huống: Data Analyst được phép sử dụng GenAI để hỗ trợ phân tích dữ liệu nhưng phải kiểm chứng kết quả, tránh hallucination và không được sao chép kết quả AI một cách máy móc.

Nhiệm vụ:

* xây dựng prompt hiệu quả;
* dùng GenAI hỗ trợ hiểu dữ liệu, đề xuất EDA, viết hoặc sửa code;
* kiểm chứng câu trả lời AI bằng Python;
* phát hiện các trường hợp AI suy diễn hoặc tạo thông tin không có trong dữ liệu;
* sử dụng GenAI hỗ trợ viết Executive Summary.

Nguyên tắc:
GenAI đề xuất $\rightarrow$ Python kiểm chứng $\rightarrow$ Con người đánh giá $\rightarrow$ Cải tiến prompt $\rightarrow$ Kết luận.

**TH03 – Big Data Analytics with PySpark**

Tình huống: Dữ liệu ngày càng lớn và việc xử lý bằng Pandas không còn phù hợp.

Nhiệm vụ: Sử dụng PySpark để:

* đọc và khám phá dữ liệu lớn;
* kiểm tra Data Quality;
* sử dụng DataFrame API;
* thực hiện Transformation và Action;
* Join nhiều bảng dữ liệu;
* thực hiện GroupBy, Aggregation và Spark SQL;
* sử dụng Window Function;
* thực hành Partition, Cache và Parquet;
* xây dựng một pipeline phân tích dữ liệu lớn.

Kết quả: Sinh viên hiểu sự khác nhau giữa Pandas và Spark và biết khi nào nên sử dụng PySpark.

**TH04 – Customer & Sales Analytics with PySpark**

Tình huống: Ban quản lý muốn hiểu hiệu quả bán hàng và hành vi khách hàng để hỗ trợ các quyết định marketing và chăm sóc khách hàng.

Nhiệm vụ:

* tính các KPI bán hàng;
* phân tích doanh thu theo thời gian, khu vực, sản phẩm và kênh;
* xây dựng Customer 360;
* phân tích Repeat Purchase;
* thực hiện RFM Analysis;
* phân nhóm khách hàng bằng $K$-Means;
* thực hiện Pareto $80/20$;
* phân tích Cohort và Retention;
* đề xuất chiến lược cho từng nhóm khách hàng.

Kết quả: Chuyển dữ liệu giao dịch thành insight phục vụ kinh doanh.

**TH05 – Investment & Stock Market Analytics with PySpark**

Tình huống: Nhà phân tích đầu tư cần xử lý dữ liệu giá của nhiều cổ phiếu để đánh giá lợi nhuận, rủi ro và danh mục đầu tư.

Nhiệm vụ:

* tính Daily Return và Annualized Return;
* tính Volatility;
* tính Sharpe Ratio;
* phân tích Drawdown;
* tính Beta;
* phân tích Moving Average;
* xây dựng Correlation Matrix;
* đánh giá Diversification;
* tính Portfolio Return và Portfolio Risk;
* so sánh các chiến lược phân bổ danh mục.

Kết quả: Viết một Executive Investment Note dựa trên số liệu phân tích.

Lưu ý: Kết quả chỉ phục vụ mục đích học tập, không phải khuyến nghị mua/bán chứng khoán.

**TH06 – Business Intelligence: PySpark + Power BI**

Tình huống: Ban lãnh đạo cần một dashboard để theo dõi hiệu quả hoạt động kinh doanh thay vì đọc dữ liệu thô.

Nhiệm vụ:

* sử dụng PySpark để ETL và tổng hợp dữ liệu;
* xây dựng mô hình Star Schema;
* tạo Fact và Dimension tables;
* xuất dữ liệu sang CSV/Parquet;
* đưa dữ liệu vào Power BI;
* thiết lập relationships;
* xây dựng các DAX Measures;
* thiết kế dashboard quản trị;
* trình bày các KPI và Executive Insights.

Sinh viên xây dựng tối thiểu các dashboard:

1. Executive Sales Overview;
2. Product & Customer Performance;
3. Management Insights.

Qua 06 bài thực hành, sinh viên cần thể hiện được quy trình:
Business Question $\rightarrow$ Data $\rightarrow$ Data Processing $\rightarrow$ Analytics $\rightarrow$ Visualization $\rightarrow$ Insight $\rightarrow$ Decision

**Yêu cầu chung**

Sinh viên cần chạy và hiểu code, không chỉ sao chép kết quả. Mỗi nhận xét phải có số liệu hoặc kết quả phân tích làm căn cứ. Khi sử dụng GenAI, sinh viên phải kiểm chứng lại bằng dữ liệu và code. Cuối mỗi bài cần trình bày các insight chính và đề xuất hành động tương ứng với tình huống kinh doanh.
