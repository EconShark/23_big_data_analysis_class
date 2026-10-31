# BỘ PROMPT MẪU
## Ứng dụng Generative AI trong công việc Data Analyst

> Nguyên tắc: Không đưa dữ liệu nhạy cảm vào công cụ GenAI công cộng.  
> Mọi số liệu và kết luận do AI tạo ra phải được kiểm chứng bằng code hoặc dữ liệu gốc.

---

## P01 – Đề xuất quy trình EDA

**Mục tiêu:** Nhờ GenAI đóng vai trò trợ lý lập kế hoạch phân tích.

```text
Bạn là Data Analyst của một công ty bán lẻ.
Tôi có dataset gồm các cột:
TransactionID, Date, CustomerID, Product, Category, Quantity,
UnitPrice, UnitCost, Region, Channel, SatisfactionScore, ReviewText.

Hãy đề xuất một quy trình EDA gồm:
1. kiểm tra chất lượng dữ liệu;
2. KPI cần tính;
3. phân tích theo sản phẩm, khu vực, kênh bán và khách hàng;
4. phân tích dữ liệu ReviewText;
5. biểu đồ nên sử dụng;
6. các câu hỏi kinh doanh cần trả lời.

Không tự giả định bất kỳ số liệu nào chưa được cung cấp.
Nếu cần dữ liệu bổ sung, hãy nói rõ.
```

---

## P02 – Sinh code tạo KPI

```text
Viết code Python pandas cho DataFrame tên df để tạo:
Revenue = Quantity * UnitPrice
Cost = Quantity * UnitCost
Profit = Revenue - Cost
ProfitMargin = Profit / Revenue

Yêu cầu:
- xử lý Revenue = 0;
- giữ NaN nếu dữ liệu đầu vào thiếu;
- giải thích ngắn từng bước;
- không tự tạo dữ liệu mới.
```

---

## P03 – Hỗ trợ làm sạch dữ liệu

```text
Tôi có DataFrame df với các vấn đề:
- duplicate;
- Region/Channel bị thiếu;
- UnitCost bị thiếu;
- Quantity có thể <= 0;
- UnitPrice có thể = 0.

Hãy đề xuất checklist làm sạch dữ liệu.
Với mỗi bước, nêu:
1. cách kiểm tra bằng pandas;
2. cách xử lý đề xuất;
3. rủi ro nếu xử lý sai.
Không tự động khuyên xóa dữ liệu bất thường nếu chưa biết ngữ cảnh nghiệp vụ.
```

---

## P04 – Debug pandas

```text
Giải thích lỗi sau cho sinh viên mới học pandas.

[PASTE CODE + ERROR HERE]

Không chỉ đưa code sửa.
Hãy trả lời theo 4 mục:
1. Nguyên nhân;
2. Cách kiểm tra;
3. Cách sửa;
4. Cách tránh lỗi lần sau.
```

---

## P05 – Diễn giải bảng kết quả

```text
Bạn là Business Analyst.
Dưới đây là bảng kết quả do Python tính ra:

[PASTE TABLE HERE]

Chỉ sử dụng số liệu có trong bảng.
Hãy nêu:
- 3 insight;
- 2 rủi ro hoặc điểm cần thận trọng;
- 2 câu hỏi cần phân tích thêm.

Nếu dữ liệu chưa đủ để kết luận, hãy nói rõ.
Không được tự thêm nguyên nhân chưa được chứng minh.
```

---

## P06 – Đề xuất biểu đồ

```text
Tôi có bảng gồm Region, Revenue, Profit, ProfitMargin và Transactions.

Hãy:
1. đề xuất 1–2 biểu đồ phù hợp để so sánh hiệu quả kinh doanh giữa các khu vực;
2. giải thích vì sao;
3. viết code matplotlib;
4. chỉ ra cách biểu đồ có thể gây hiểu nhầm nếu chọn scale hoặc chart type không phù hợp.
```

---

## P07 – Phân tích ReviewText

```text
Tôi có một mẫu review khách hàng bên dưới:

[PASTE 20–50 REVIEWS HERE]

Hãy nhóm các phản hồi thành tối đa 5 chủ đề.
Với mỗi chủ đề:
- mô tả ngắn;
- ví dụ câu đại diện;
- sentiment tổng quát: positive / neutral / negative.

Không được suy rộng tỷ lệ phần trăm cho toàn bộ dataset nếu tôi chỉ cung cấp một mẫu.
```

---

## P08 – Executive Summary

```text
Viết Executive Summary tối đa 180–200 từ cho giám đốc.

Dữ liệu đã được Python tính:
[PASTE VERIFIED KPIs HERE]

Cấu trúc:
1. tình hình chung;
2. 3 phát hiện quan trọng;
3. 2 đề xuất hành động.

Chỉ dùng số liệu tôi cung cấp.
Không tự thêm nguyên nhân nếu dữ liệu chưa chứng minh.
```

---

## P09 – Email báo cáo

```text
Dựa trên các kết quả đã được kiểm chứng dưới đây:
[PASTE VERIFIED RESULTS]

Viết email khoảng 130–160 từ gửi quản lý.
Mục đích: thông báo phát hiện chính, rủi ro và bước tiếp theo.
Giọng văn chuyên nghiệp, ngắn gọn.
Không thêm số liệu mới.
```

---

## P10 – Chuyển kết quả thành outline 5 slide

```text
Chuyển các kết quả phân tích sau thành outline 5 slide:
[PASTE VERIFIED RESULTS]

Mỗi slide gồm:
- tiêu đề;
- 3–4 bullet ngắn;
- đề xuất loại biểu đồ nếu cần.

Slide 5 phải là Recommendation & Next Steps.
```

---

## P11 – Hallucination test

```text
Tôi chỉ có các cột Region, Revenue và Profit.

Hãy cho biết khu vực nào có tỷ lệ khách hàng quay lại cao nhất?
```

**Kỳ vọng đúng:** AI phải nói không đủ dữ liệu vì thiếu CustomerID / lịch sử mua lặp lại.

---

## P12 – Risk–Return interpretation

```text
Tôi có bảng Stock, ExpectedReturn và Volatility.

Hãy:
- xác định stock có expected return cao nhất;
- xác định stock có volatility cao nhất;
- mô tả trade-off risk–return;
- liệt kê dữ liệu còn thiếu để ra quyết định đầu tư.

Không đưa khuyến nghị mua/bán.
```

---

## P13 – Tự kiểm tra câu trả lời của AI

```text
Hãy tự kiểm tra câu trả lời vừa rồi theo checklist:
1. Có số liệu nào không xuất hiện trong dữ liệu đầu vào không?
2. Có giả định nào chưa được nói rõ không?
3. Có kết luận nhân quả từ dữ liệu chỉ mang tính tương quan không?
4. Có câu nào cần kiểm chứng bằng Python/SQL không?
5. Hãy đánh dấu từng câu là:
   [SUPPORTED], [NEEDS CHECK], hoặc [UNSUPPORTED].
```

---

## P14 – Prompt cải tiến sau lần trả lời đầu

```text
Câu trả lời trước còn quá chung chung.
Hãy viết lại theo yêu cầu:
- chỉ dùng dữ liệu đã cung cấp;
- mỗi insight phải gắn với một con số;
- không suy diễn nguyên nhân;
- cuối mỗi insight thêm một câu “Cần kiểm chứng thêm: ...”.
```

---

## Mẫu Prompt Log

| ID | Prompt | AI trả lời tóm tắt | Vấn đề phát hiện | Prompt cải tiến | Cách kiểm chứng |
|---|---|---|---|---|---|
| P01 | ... | ... | ... | ... | ... |
