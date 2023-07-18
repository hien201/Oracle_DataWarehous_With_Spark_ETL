# Oracle_DataWarehouse_With_Spark_ETL
### 1. Giới thiệu dự án:
Văn phòng Lữ hành và Du lịch Quốc gia (NTTO) quản lý chương trình khách đến ADIS/I-94 với sự hợp tác của Bộ An ninh Nội địa (DHS)/Hải quan và Bảo vệ Biên giới Hoa Kỳ (CBP). I-94 cung cấp số lượng khách du lịch đến Hoa Kỳ (với thời gian lưu trú từ 1 đêm trở lên và đến thăm theo một số loại thị thực nhất định) để tính toán xuất khẩu khối lượng du lịch và du lịch của Hoa Kỳ.

Với vai trò là một kỹ sư dữ liệu, tôi muốn xây dựng mô hình dữ liệu I-94 để người dùng cuối có thể thực hiện phân tích thống kê như: Thành phố nào được truy cập nhiều nhất ở Hoa Kỳ? Trong bao nhiêu ngày? Vì lý do gì. Và nó phát triển như thế nào? Nó có liên quan đến các yếu tố khác như nhiệt độ hoặc dân số của thành phố không?

Tôi sẽ tập chung vào 2 phần chính:
- Phân tích yêu cầu và dữ liệu nguồn để thiết kế DW phù hợp.
- Xây dựng đường ống dữ liệu ETL đảm bảo các yêu cầu về khai thác và bài toán về gia tăng khối lượng dữ liệu nguồn trong quá trình vận hành theo kịch bản.

----------------------------
### 2. Xác định yêu cầu và scop dự án:
#### Yêu cầu:
- Đảm bảo tính toàn vẹn của dữ liêu
- Chuẩn hóa và xử lý những vấn đề của dữ liệu nguồng
- Đảm bảo khả năng hoạt động ổn định của ELT khi lượng dữ liệu nguồn tăng lên 
#### Scop dự án:
* Phân tích và thiết kế DW
* Sau đó,  xây dựng một ETL pipeline để load dữ liệu vào DW (Oracle) từ 5 nguồn: 
     * I94 Immigration data (lưu trữ ở DB postgres)
     * Temparture Data (lưu trữ ở local)
     * U.S City Demographic Data (lưu trữ ở local)
     * City Wether Data (lưu trữ ở local)
    * mapping txt file (lưu trữ ở local) 
* Sử dụng công nghệ: 
    * Posgre: Database
    * Oracle: Data Warehouse
    * Spark (pyspark): ETL dữ liệu kích thước lớn
    * Pandas: ETL dữ liệu kích thước nhỏ
-----------------------------------
### 3. Thu thập và khai phá dữ liệu:
#### Thu thập dữ liệu: (nguồn lấy dữ liệu) 
* I94 Immigration: 
* Tempature Data:
* 
#### Khai phá dữ liệu: mục đích là tìm hiểu xem các dữ liệu này cung cấp thông tin gì
###### I94 Immigration: 
* Cung cấp thông tin về nhập cư tại Hoa Kỳ như danh sách nhập cư, cảng nhập cảnh, ngày đến, ngày đi, địa điểm đến, loại visa….
* Trong dự án này, tôi sử dụng dữ liệu tháng 4 năm 2016. Đây sẽ là bảng gốc của bảng TS_IMGT_F 
###### Temperature Data: 
* Cung cấp thông tin về nhiệt độ thế giới tại các thành phố từ năm 1974 đến năm 2018


###### Airport Data: 
* Cung cấp dữ liệu về các sân bay như mã sân bay, tên sân bay trên thế giới.
###### U.S City Demographic Data: 
* Dữ liệu về nhân khẩu học tại các thành phố tại hoa kì có dân số lớn hơn hoặc bằng 65.000 người.
* Tôi muốn kiểm tra mối quan hệ giữa nhân khẩu học của các thành phố và lượng du khách/ người nhập cư đến đó.  
###### Mapping file:
* chứa dữ liệu cho một số bảng dim: country, visa, port, state, mode 
##### Đánh giá dữ liệu: 
* Tôi phải xem xét về chất lượng và các vấn đề dữ liệu gặp phải. Từ đó xây dựng phương án xử lý các vấn đề này ( mô tả chi tiết tại mục 4)
------------------------------------
### 4. Xác định các bảng trong DW:
 Cần phải xác định được DW sẽ được tổ chức như thế nào? gồm những bảng nào?
#### Trong dự án này, tôi xác định DW sẽ có:
* 2 vùng: Staging và Mart
* 2 loại bảng: Dim và Fact
* Tổng cộng 14 bảng
  
![image](https://github.com/hien201/Oracle_DataWarehous_With_Spark_ETL/assets/90466915/499c3140-58ff-4f37-8b2a-1719e10dfb96)
* Các bảng cần được xác định rõ thông tin, ví dụ:
  
![image](https://github.com/hien201/Oracle_DataWarehous_With_Spark_ETL/assets/90466915/f2efb28f-5f73-4575-92ee-7a3fd40ac5e2)

-----------------------------------------
### 5. Xây dựng Data Model:
Sau khi xác định được các bảng trong DW, tôi tiến hành xác định quan hệ giữa các bảng
=> Sau đó, xây dựng Mô hình quan hệ như sau: 

![image](https://github.com/hien201/Oracle_DataWarehous_With_Spark_ETL/assets/90466915/9e130474-09d6-4190-be94-5708a1eac3ee)

-----------------------------------------------
### 6. Các vấn đề dữ liệu và phương án xử lý:
#####  I94 Immigration:
- Có nhiều column không cần thiết, đồng thời những column này cũng thường xuyên thiếu dữ liệu
    ⇒ Xóa các column không cần thiết 
- Nhiều bản ghi trùng lặp
    ⇒ Check trùng và xóa đi.
- Có nhiều I94port và addr không hợp lệ, thiếu \n
    => Mapping với file txt và đưa các bản ghi sai lệch thành “O” viết tắt của “OTHER”
- Trường dtadfile có giá trị < 20160101. Mà dữ liệu lấy trong năm 2016 do đó dtadfile không thể < 20160101 được. 
    ⇒ xóa bản ghi có dtadfile < 20160101
- To_char(YR, “YYYY”) khác “2016”
    ⇒ xóa bản ghi.
- Định dạng thời gian trường arrdate ở dạng ssas => Cần đưa về định dạng Datetime.
  
##### Airport:
- Thừa dữ liệu vì data này cung cấp dữ liệu về sân bay trên toàn thế giới => chỉ lấy dữ liệu sân bay ở US.
- Có nhiều sân bay đóng cửa => xóa bản ghi
- Chồng lấn dữ liệu trường iso_region => tách  state_code và country_code
  
##### Port (dim mapping txt):
- Tên port và tên state nằm cùng một trường => tách ra
- Có nhiều port không xác định => chuyển về "other"
- Chỉ lấy những port thuộc US => mapping với state table

  ------------------------------------
7. Xác định chương trình và phát triển chương trình:
#### Thiết kế kiến trúc công nghệ:

![image](https://github.com/hien201/Oracle_DataWarehous_With_Spark_ETL/assets/90466915/ca0270a0-0d24-4ac5-a227-86471adc2a83)

#### Xác định và xây dựng các chương trình dựa trên kiến trúc công nghệ:
*  Step 1: Prepare: Load toàn bộ data vào postgres, bao gồm: I94-Immigration, temperature, city demo và các file txt mapping.
* Step 2: Extract and Load toàn bộ data từ postgres vào các Pyspark DataFrame: df_Immigration, df_citi_demo, df_temp, df_port, df_airport, df_contry, df_visa
* Step 3: Clean theo mục 4 và load data từ các df_Immigration, df_citi_demo, df_temp vào staging table DW
* Step 4: Clean theo mục 4 và load data từ các df: df_port, df_airport, df_contry, df_visa vào vùng mart luôn, không cần qua vùng staging. 
* Step 5: Transfer (sinh thêm bảng date từ bảng Immigration) và load data từ staging table sang mart table:
    * Join bảng I94 Immigration với Temp thành bảng I94_visitor
    * Insert data từ bảng I94 Immigration vào bảng date 
* Kiểm tra chất lượng dữ liệu:
    * Kiểm tra bản ghi trùng lặp trong bảng I94_visitor
    * Kiểm tra các vấn đề về dữ liệu đã được xử lý chưa
=> Sau khi xác định được các bước rồi thì tiến hành xây dựng các chương trình tương ứng

#### Thiết kế Batch:
ETL sẽ được chạy theo Batch sau: 

![image](https://github.com/hien201/Oracle_DataWarehous_With_Spark_ETL/assets/90466915/643dcb19-58de-4328-9303-b066d18eca88)

-------------------------------
### 8. Project Structure:
![image](https://github.com/hien201/Oracle_DataWarehous_With_Spark_ETL/assets/90466915/477eb6f7-5b37-4a91-9121-9b9ae60bedcc)







     

     
 
    



