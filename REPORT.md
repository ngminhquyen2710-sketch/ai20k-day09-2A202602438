# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Minh Quyền |
| MSSV | 2A202602438 |
| GitHub | ngminhquyen2710-sketch |
| Trip dùng cho bài tập sập và chạy lại | trip-06667 |

## Kết quả cuối

- `dataset_version` của bạn: `0e05ca3103f7`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `693 train / 177 val / 149 test` (tổng 1019)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Dựa vào dữ liệu thô trích xuất từ hai bảng bronze (`raw_telemetry_a` và `raw_telemetry_b`), có các khác biệt rõ rệt sau:
1. **Định dạng thời gian:** Nguồn A dùng chuẩn chuỗi ISO 8601 UTC (`ts_utc`, ví dụ `2025-10-07T08:00:00.014Z`), trong khi Nguồn B dùng Unix timestamp tính bằng mili-giây (`timestamp_ms`, ví dụ `1759933801509`).
2. **Đơn vị và tên cột vận tốc:** Nguồn A đặt tên cột là `speed_mps` đo bằng mét/giây (m/s); Nguồn B đặt tên là `spd_kmh` đo bằng km/h.
3. **Tên cột và thang đo điều khiển (Ga/Phanh):** Nguồn A dùng `gas` và `brake` theo thang chuẩn hóa [0.0, 1.0]; Nguồn B dùng `throttle_pct` và `brake_pct` theo thang phần trăm [0, 100%].
4. **Quy ước đặt tên cột GPS và cảm biến:** Nguồn A dùng tên ngắn gọn (`lat`, `lon`, `alt_m`, `heading_deg`, `acc_x`, `acc_y`, `steer`), còn Nguồn B dùng tên khác (`latitude`, `longitude`, `altitude`, `hdg`, `ax`, `ay`, `steering`). Ngoài ra, bảng `bronze_video_files` cho thấy video B bị lệch đồng hồ `telemetry_clock_offset_ms = 1500 ms` so với telemetry, trong khi A có offset = 0.

Pipeline giữ **mỗi nguồn một bảng bronze riêng biệt và giữ nguyên dạng chữ (raw string)** vì tuân thủ nguyên lý Medallion Architecture:
- **Bảo toàn nguyên bản (Raw Data Fidelity):** Tầng Bronze đóng vai trò là "Single Source of Truth" bất biến. Việc giữ kiểu chuỗi giúp ingest an toàn 100% bản ghi mà không bao giờ bị crash bởi lỗi cú pháp, ô trống, dữ liệu dị thường (anomaly) hay ép kiểu thất bại.
- **Cách ly Schema Drift:** Mỗi nguồn thiết bị/cảm biến từ các nhà cung cấp khác nhau có cấu trúc độc lập. Không gộp sớm giúp việc thay đổi schema ở một nguồn không làm vỡ các luồng của nguồn khác.
- **Khả năng tái xử lý (Replay/Audit):** Dữ liệu gốc được bảo toàn nguyên vẹn cho phép ta thử nghiệm, sửa đổi logic chuẩn hóa hoặc backfill dữ liệu ở tầng Silver bất kỳ lúc nào mà không bị mất mát thông tin ban đầu.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

**1. Cách pipeline chuẩn hóa hai schema thành một (qua staging dbt):**
- **Đơn vị vận tốc:** Nguồn A đã ở dạng m/s; nguồn B dùng km/h được quy đổi sang m/s bằng công thức `try_cast(nullif(spd_kmh, '') as double) / 3.6`.
- **Thang đo chân ga/phanh:** Nguồn B chia cho 100 (`throttle_pct / 100`, `brake_pct / 100`) để đồng nhất về khoảng [0.0, 1.0] của nguồn A.
- **Tên cột:** Đồng nhất các cột tọa độ và cảm biến (`latitude` -> `lat`, `longitude` -> `lon`, `altitude` -> `alt_m`, `hdg` -> `heading_deg`, `ax` -> `acc_x`, `ay` -> `acc_y`, `steering` -> `steer`).
- **Kiểu thời gian và trừ độ lệch đồng hồ:** Nguồn A chuyển chuỗi ISO thành `timestamptz`. Nguồn B lấy Unix timestamp mili-giây trừ đi `telemetry_clock_offset_ms` (1500 ms từ `bronze.video_files`) trước khi chia 1000 để đổi thành timestamp UTC: `to_timestamp((timestamp_ms - 1500) / 1000.0)`.

**2. Bằng chứng kết quả bước 2:**
- Thời điểm bắt đầu của telemetry B (`bat_dau`) sau khi trừ offset là `2025-10-08 14:30:00.009000+00:00`, lệch chưa tới 10ms so với mốc khởi đầu video B (`2025-10-08 14:30:00`). Test `drive B clock offset removed` đạt PASS.
- Vận tốc tối đa sau khi quy đổi sang km/h của cả hai drive đều thực tế và hợp lý (Drive A: 71 km/h, Drive B: 100 km/h).

**3. Bảng quarantine (dòng bị cách ly):**

| drive_id | reject_reason | so_dong |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

Tổng số dòng cách ly là 53 dòng mỗi drive (tổng 106 dòng trên toàn bộ). Tổng số dòng sạch + cách ly khớp chính xác 100% số dòng nạp vào: Drive A (2968 + 53 = 3021), Drive B (3181 + 53 = 3234).

**4. Vì sao dòng lỗi vào `rejects` thay vì bị xóa âm thầm (Dead Letter Queue / Quarantine pattern):**
- **Tránh mất mát thông tin:** Xóa dòng lỗi sẽ làm mất dấu vết (loss of signal). Việc lưu vào bảng `silver.telemetry_rejects` cho phép kỹ sư dữ liệu giám sát và truy vết nguyên nhân lỗi từ phần cứng/cảm biến (ví dụ: cảm biến GPS bị treo gửi lặp, hoặc cảm biến tốc độ rớt mạng).
- **Khả năng khắc phục (Remediation/Replay):** Nếu sau này phát hiện ra luật lọc bị quá chặt hoặc có thể suy luận lại giá trị (imputation), ta có thể viết logic sửa lỗi và tái tích hợp các dòng này mà không cần crawl lại dữ liệu từ đầu.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

**1. Phân biệt mức độ cảnh báo `warn` và `error`:**
- **Test `max_gap_ms` đặt là `warn`:** Khoảng cách thời gian giữa hai bản ghi đo liên tiếp vượt quá 1000ms phản ánh hiện tượng cảm biến bị mất tín hiệu tạm thời (dropout/packet loss). Đây là khiếm khuyết vật lý thực tế ngoài hiện trường xe chạy và không làm sai lệch tính toàn vẹn của các bản ghi khác; pipeline vẫn có thể xử lý và chấp nhận được nên chỉ cảnh báo để theo dõi, không dừng toàn bộ hệ thống.
- **Test `in_range` đặt là `error`:** Vận tốc vượt ngưỡng [0, 70 m/s] (~252 km/h) là giá trị sai lệch phi vật lý của xe ô tô thông thường. Dữ liệu này nếu lọt vào Gold sẽ gây ra nhiễu nghiêm trọng (garbage-in, garbage-out) cho mô hình Computer Vision khi học liên hệ giữa hình ảnh và trạng thái xe. Vì vậy đây là lỗi vi phạm nghiêm trọng (fatal), bắt buộc phải dừng pipeline (block downstream) để bảo vệ dữ liệu đích.

**2. Kết quả thí nghiệm `break-gate` (`break_gate.json`):**
- Khi nới luật làm sạch từ 70 m/s lên 1000 m/s, các giá trị tốc độ vô lý không còn bị bảng quarantine chặn lại mà lọt thẳng vào `silver.telemetry`.
- Ngay lập tức, bài test dbt chất lượng `in_range_silver_telemetry_speed_mps__70__0` đã phát hiện và chặn đứng với kết quả: **ERROR=1 (bắt được chính xác 6 dòng vi phạm - 3 dòng từ drive_a và 3 dòng từ drive_b)**, tổng trạng thái test chuyển thành `PASS=5 WARN=1 ERROR=1 SKIP=0`.
- Sau khi khôi phục luật gốc (70 m/s), test quay trở lại đạt chuẩn `PASS=6 WARN=1 ERROR=0 SKIP=0`.

**3. Vai trò của Cổng chất lượng (Quality Gate):**
Thí nghiệm chứng minh Cổng chất lượng đóng vai trò như một **hàng rào phòng thủ đa tầng (Defense in Depth)** độc lập với logic biến đổi dữ liệu. Ngay cả khi code biến đổi hoặc bộ lọc upstream gặp lỗi/bị nới lỏng ngoài ý muốn (human error / buggy transform), cổng kiểm định downstream vẫn là chốt chặn cuối cùng ngăn chặn dữ liệu bẩn xâm nhập vào Data Warehouse và Gold dataset.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

**1. Số liệu thực nghiệm từ `idempotency.json` (thí nghiệm trên `trip-00003`):**
- **Trước khi chạy lại:** Bảng có `1246` dòng, mã băm dấu vân tay (fingerprint) là `6c7480dfde7e`.
- **Sau 2 lần chạy lại partition:** Bảng vẫn giữ nguyên chính xác `1246` dòng, dấu vân tay không đổi `6c7480dfde7e` (`unchanged = true`).
- **Khi mô phỏng ghi sai kiểu `append`:** Số dòng của trip thử nghiệm bị nhân đôi từ 60 dòng lên `120` dòng (`append_variant_rows_after_two_runs = 120`), gây rác và trùng lặp dữ liệu nghiêm trọng.

**2. Vì sao Delta `overwrite` theo partition an toàn hơn cơ chế xoá rồi ghi lại (delete-then-insert):**
- **Đảm bảo tính nguyên tử (ACID Atomicity):** Trong Delta Lake, cơ chế overwrite theo partition (`replaceWhere`) sẽ ghi toàn bộ các file Parquet mới vào bộ nhớ trước khi thực hiện một commit nguyên tử duy nhất vào transaction log (`_delta_log`). Nếu có sự cố (crash container, lỗi mạng, OOM) giữa chừng khi đang trích xuất, giao dịch bị hủy và bảng vẫn giữ nguyên trạng thái phiên bản cũ hợp lệ mà không bị hỏng.
- **Tránh mất dữ liệu (Zero Data Loss Window):** Nếu dùng phương pháp "xóa trước rồi ghi sau", khoảng thời gian giữa lệnh delete và insert là lỗ hổng chết người; nếu tiến trình chết giữa chừng, toàn bộ partition của trip đó sẽ bị mất vĩnh viễn, khiến các pipeline hạ tầng downstream đọc phải bảng rỗng.
- **Cô lập tác vụ (Partition Isolation):** Lệnh overwrite theo partition chỉ tác động cục bộ lên đúng thư mục/file của trip đó, không can thiệp hay khóa bảng (locking) đối với 25 trip còn lại đang chạy song song.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

**1. Khung hình đen vs Khung hình tối và cách xử lý:**
- **Khung hình đen (`is_black`):** Có độ sáng trung bình cực thấp (mean luma < 12.0), xuất phát từ lỗi phần cứng (nắp ống kính chưa mở, camera mất tín hiệu phơi sáng). Khung đen chứa 0% thông tin hình ảnh. Pipeline **loại bỏ hoàn toàn (drop)** khỏi tập Gold.
- **Khung hình tối (`is_dark` / `toi_giu_lai`):** Có ánh sáng yếu nhưng vẫn nhìn thấy được (xe chui qua hầm, bóng râm cây cối, chạng vạng). Vẫn chứa các đường nét quan trọng (vạch kẻ đường, đèn xe khác, biển báo). Pipeline **gắn cờ cảnh báo nhưng giữ lại** (drive_b có tới 61 khung tối được giữ lại, drive_a có 4 khung).
- **Tôi hoàn toàn đồng ý** với thiết kế này: Mô hình lái xe tự hành bắt buộc phải nhận diện được vật cản trong điều kiện ánh sáng yếu/ban đêm; việc giữ lại khung tối giúp mô hình học được phân phối thực tế, trong khi loại khung đen giúp tránh làm loãng gradient.

**2. Bảng số lượng khung trùng theo ngưỡng dHash từ `threshold.json`:**

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

**3. Lựa chọn ngưỡng và sự phụ thuộc vào tốc độ xe:**
- **Tôi chọn ngưỡng 2 bit (như pipeline đang dùng):** 
  - Ngưỡng 0 bit quá khắt khe: do nhiễu cảm biến (sensor noise) hoặc nén video, hai khung xe đứng yên vẫn có thể lệch 1 bit, dẫn đến bỏ sót nhiều khung trùng (A chỉ bắt 61 khung thay vì 149).
  - Ngưỡng 6 bit quá lỏng: loại bỏ quá mức (363/602 khung ở A, 244/644 ở B, mất hơn 50% dataset), làm mất cả những khung hình xe đang di chuyển bình thường có cảnh quan thay đổi nhẹ. Ngưỡng 2 là điểm cân bằng lý tưởng (sweet spot).
- **Phụ thuộc vào vận tốc xe:** Khi xe chạy chậm hoặc dừng đèn đỏ (drive A có tốc độ trung bình thấp hơn ~41 km/h và bị camera đứng hình 6s), cảnh vật giữa hai khung hình liên tiếp (cách nhau 0.5s) gần như y hệt nhau, khoảng cách dHash rất nhỏ ($\le 2$ bit) nên số khung trùng rất cao (149 khung). Khi xe chạy nhanh trên xa lộ (drive B có tốc độ cao 71-100 km/h), cảnh quan dịch chuyển rất nhanh tạo ra biến động dHash lớn, do đó số khung bị coi là trùng ít hơn nhiều (chỉ 53 khung).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

**1. Số liệu thực nghiệm từ `leakage.json` (trên tổng số 1019 khung hình thuộc 26 trip):**
- **Chia theo khung hình (Random frame-level split 70/15/15):** Có **26/26 trip (100%)** bị phân mảnh nằm ở nhiều hơn một split (`per_frame_split_trips_straddling = 26`), và **toàn bộ 26/26 trip** đều có khung hình xuất hiện đồng thời ở cả tập Train lẫn tập Test (`per_frame_split_trips_in_both_train_and_test = 26`).
- **Chia theo trip (Trip-level split của pipeline):** Có **0 trip bị chia cắt** (`per_trip_split_trips_straddling = 0`). Mỗi trip nằm trọn vẹn và duy nhất trong một split (Train 18 trips, Val 4 trips, Test 4 trips).

**2. Vì sao chia theo khung hình làm điểm kiểm thử đẹp giả tạo (Data Leakage):**
- Trong bài toán Computer Vision cho xe tự hành, các khung hình liên tiếp trích xuất từ video (cách nhau chỉ 0.5 giây) có độ tương quan hình ảnh cực kỳ cao (cùng ánh sáng, cùng thời tiết, cùng đoạn đường và các xe xung quanh).
- Nếu chia theo khung hình, mô hình đã "học thuộc" cảnh nền của một chuyến đi trong tập Train, và khung hình ở tập Test chỉ là một biến thể xê dịch vài mét của cảnh đó. Mô hình chỉ đang ghi nhớ cục bộ (memorization) thay vì học tính khái quát hóa (generalization). Kết quả là điểm kiểm thử (loss, mAP) trên tập Test cao một cách giả tạo, nhưng mô hình sẽ thất bại hoàn toàn khi đem triển khai thực tế trên một chuyến đi mới lạ.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

**1. Vì sao `dataset_version` là băm nội dung (Content-addressed Hash):**
- **Đảm bảo tính tái lập (Reproducibility & Idempotency):** Băm nội dung biến dataset thành một giá trị định danh bất biến (Content-addressable). Cùng một tập dữ liệu đầu vào đã lọc và cùng cách phân chia sẽ luôn sinh ra cùng một mã băm duy nhất. Nếu dùng ngày giờ (timestamp) hoặc số đếm tự tăng (auto-increment), mỗi lần chạy lại pipeline (dù không đổi một byte dữ liệu nào) hệ thống sẽ tạo ra một version mới, làm rác kho lưu trữ và mất khả năng so sánh/chống trùng lặp giữa các lần huấn luyện mô hình.
- **Kết quả hai lần chạy liên tiếp:** Cho ra chính xác cùng một mã băm `dataset_version` (`0e05ca3103f7`).

**2. Vì sao `dataset_version` của tôi khác bạn bè dù cùng bộ dữ liệu:**
- Trong model `gold_dataset_frames.sql`, phép gán split cho các trip sử dụng `LAB_STUDENT_ID` (MSSV của tôi là `2A202602438`) làm muối băm (salt): `order by md5(trip_id || '{{ env_var("LAB_STUDENT_ID", "") }}')`.
- Mỗi sinh viên có một MSSV khác nhau, dẫn đến chuỗi MD5 khác nhau, khiến thứ tự xếp hạng và phân phối 26 trip vào 3 tập train/val/test của mỗi sinh viên là hoàn toàn khác biệt. Tập khung hình ở các split khác nhau dẫn tới file `manifest.json` khác nhau, sinh ra mã băm `dataset_version` độc nhất vô nhị cho từng người.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

**1. Trả lời "dataset này được tạo từ dữ liệu nào?" dựa trên `manifest.json` (phiên bản `0e05ca3103f7`):**
Dataset được tạo trực tiếp từ các phiên bản bảng Delta và tệp video gốc sau:
- **Tầng Bronze:** `bronze.raw_telemetry_a` (version 0), `bronze.raw_telemetry_b` (version 0), `bronze.trips_raw` (version 0), `bronze.video_files` (version 1).
- **Tầng Silver:** `silver.frames` (version 27), `silver.telemetry` (version 0), `silver.frames_flagged` (version 0), `silver.frame_telemetry` (version 0).
- **Video gốc (SHA256):**
  - Drive A: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
  - Drive B: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`
- **Bộ quy tắc áp dụng (`rules`):** Loại bỏ khung hình thỏa mãn `is_black`, `is_blurry`, `is_near_dup`, `not sync_ok`; phân chia split theo trip phân tầng theo drive.

**2. Các thông tin giúp tái tạo chính xác 100% dataset này sau 3 tháng:**
- Hai tệp video và các tệp telemetry gốc có mã SHA256 khớp tuyệt đối với thông tin lưu ở `lineage`.
- Số phiên bản cụ thể (version) của các bảng Delta tại thời điểm tạo (cho phép dùng tính năng Delta Lake Time Travel để query lại đúng trạng thái đó).
- Giá trị biến môi trường `LAB_STUDENT_ID=2A202602438` (đóng vai trò seed muối băm quyết định cách phân bố split).
- Tỷ lệ phân chia split (Train 70%, Val 15%, Test 15%) và các ngưỡng lọc chất lượng đã cấu hình.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

**1. Diễn biến khi trip được chỉ định (`trip-06667`) sập và chạy lại:**
- **Số task chuyển đỏ:** Khi thiết lập `LAB_FAIL_ONCE=trip-06667`, trong cụm dynamic mapping 26 task `silver_frames`, có đúng **1 task** chuyển sang màu **đỏ (FAILED)** tương ứng với partition `trip-06667`. Log của task báo lỗi có kiểm soát: `simulated crash while extracting trip-06667`. 25 task trích xuất khác của 25 trip còn lại vẫn hoàn thành bình thường (màu xanh).
- **Số task bị `upstream_failed`:** Toàn bộ **7 task** hạ nguồn phụ thuộc vào `silver_frames` bao gồm: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse` bị chặn thực thi và chuyển sang trạng thái **`upstream_failed` (màu cam)**.
- **Thao tác xử lý (Clear):** Trên giao diện Airflow Graph, ta chọn task đỏ duy nhất (`silver_frames` của `trip-06667`) và nhấn **Clear** (chọn `downstream = true`). Task này được lên lịch chạy lại thành công (xanh); ngay sau đó Airflow tự động giải phóng trigger và tuần tự chạy thành công cả 7 task downstream (toàn bộ DAG chuyển xanh hoàn toàn).
- **Dữ liệu có bị nhân đôi không:** **Hoàn toàn KHÔNG**. Pipeline sử dụng cơ chế Delta Lake `replaceWhere = "trip_id = 'trip-06667'"` (ghi đè theo partition). Khi chạy lại lần thứ hai, các bản ghi cũ của `trip-06667` được thay thế nguyên tử (atomic overwrite), không xảy ra tình trạng duplicate rows.
- **`dataset_version` có đổi không:** **KHÔNG ĐỔI**, vẫn giữ nguyên chính xác là `0e05ca3103f7`.
- **Bằng chứng số liệu & hình ảnh:** 
  - Toàn bộ 27/27 kiểm tra chất lượng vẫn đạt PASS tuyệt đối (`verify_all_passed: true`).
  - Tổng số lượng khung hình vẫn cố định 1019 khung hình (693 train / 177 val / 149 test), khớp 100% với `report/evidence/run_summary.json`.
  - Ảnh chụp màn hình: `report/screenshots/airflow_task_crash.png` (log task đỏ chứa `simulated crash while extracting trip-06667`) và `report/screenshots/airflow_graph.png` (toàn bộ DAG xanh).

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi quy mô tăng lên 500 xe/ngày (~hàng nghìn giờ video, hàng triệu khung hình và hàng chục GB telemetry/ngày), ba thành phần sau sẽ đổ vỡ đầu tiên:
1. **Bộ giải mã và trích xuất video cục bộ (FFmpeg / OpenCV trên worker đơn lẻ - Slide 23, 24):**
   - *Vấn đề:* Tác vụ giải mã video (decode) và trích xuất đặc trưng hình ảnh (dHash, Laplacian variance) tiêu tốn năng lực CPU/RAM cực lớn. Hiện tại Airflow chạy tuần tự/cục bộ trên 1 container worker với cấu hình `max_active_tis_per_dagrun=1`. Với 500 xe, quá trình này sẽ mất hàng chục đến hàng trăm giờ để hoàn tất và đối mặt nguy cơ OOM crash ngay lập tức.
   - *Giải pháp thay thế (Slide 23):* Thay thế bằng cụm tính toán phân tán (Distributed compute cluster) chuyên dụng cho media processing như **Ray** hoặc sử dụng **Airflow Celery/KubernetesExecutor** kết hợp cơ chế worker autoscaling (hàng trăm pod xử lý song song), hoặc đẩy sang dịch vụ serverless batch (như AWS Batch).
2. **Engine chuyển đổi dữ liệu DuckDB in-process cục bộ (Slide 22):**
   - *Vấn đề:* DuckDB là in-process engine lưu trên 1 file `lakehouse.duckdb`. Nó bị giới hạn chặt chẽ bởi bộ nhớ RAM của một máy vật lý và cơ chế khóa file đơn (single-writer lock). Khi bảng telemetry đạt hàng trăm triệu dòng và dbt thực hiện các phép join event-time phức tạp giữa video và telemetry, DuckDB sẽ cạn kiệt tài nguyên RAM và xung đột truy cập ghi đồng thời (concurrency conflict).
   - *Giải pháp thay thế (Slide 12, 22):* Thay DuckDB bằng các công cụ tính toán phân tán MPP/Lakehouse như **Apache Spark (PySpark / Delta-Spark)**, **Trino/Presto**, hoặc **Databricks**, kết hợp cùng dbt adapter tương ứng (`dbt-spark` / `dbt-databricks`) để chia nhỏ và xử lý song song trên nhiều worker node.
3. **Kho lưu trữ MinIO single-node và Postgres Data Warehouse (Slide 12, 16):**
   - *Vấn đề:* MinIO chạy trên 1 container gắn volume đĩa cục bộ sẽ nhanh chóng cạn kiệt dung lượng đĩa cứng (hàng chục TB video gốc và delta parquet mỗi tuần) và nghẽn băng thông I/O đường truyền. Postgres truyền thống cũng sẽ gặp trần chịu tải (bottleneck) khi phải đóng vai trò vừa là data warehouse lưu hàng triệu bản ghi vừa phục vụ truy vấn BI đồng thời từ người dùng.
   - *Giải pháp thay thế (Slide 12, 16):* Chuyển vùng hạ tầng lake sang dịch vụ lưu trữ đối tượng đám mây chuẩn cấp doanh nghiệp (**AWS S3 / Google Cloud Storage**) tích hợp Lifecycle policies (chuyển video cũ sang Cold/Glacier tier để tiết kiệm chi phí); đồng thời thay Postgres bằng Cloud Data Warehouse phân tán có kiến trúc tách rời tính toán và lưu trữ (Decoupled Compute & Storage) như **Snowflake**, **Google BigQuery**, hoặc **AWS Redshift**.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
select 
    trip_id,
    drive_id,
    lighting,
    conditions,
    frames as tong_khung,
    frames_kept as khung_giu_lai,
    (frames - frames_kept) as khung_bi_loai,
    round(100.0 * (frames - frames_kept) / frames, 2) as ty_le_loai_pct,
    split
from marts.trip_summary
order by ty_le_loai_pct desc;
```

**Điều câu truy vấn này cho biết:**
- Câu SQL trích xuất từ bảng `marts.trip_summary` nhằm tính toán tỷ lệ khung hình bị loại bỏ (`ty_le_loai_pct`) trên từng chuyến xe (trip), đồng thời phân tầng theo điều kiện ánh sáng (`lighting`) và điều kiện đường sá/thời tiết (`conditions`).
- **Ý nghĩa thực tiễn đối với kỹ sư dữ liệu / ML:** Giúp đội ngũ giám sát chất lượng thu thập dữ liệu xác định ngay những chuyến xe có tỷ lệ hao hụt cao bất thường. Chẳng hạn, các trip đi qua hầm tối hoặc dừng xe kéo dài (như chờ đèn đỏ) sẽ có tỷ lệ loại bỏ rất cao do rơi vào bộ lọc khung đen (`is_black`) hoặc khung gần trùng lặp (`is_near_dup`). Ngược lại, các trip di chuyển liên tục trên cao tốc ban ngày giữ lại được >95% số khung. Từ đó, ta có cơ sở để tối ưu hóa tần số lấy mẫu FPS của camera và điều chỉnh lộ trình thu thập dữ liệu cho các đợt chạy xe tiếp theo.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- **Bất ngờ nhất:** Cơ chế phân chia split theo cấp độ chuyến đi (Trip-level split) bảo vệ dataset triệt để đến mức triệt tiêu hoàn toàn 100% rò rỉ dữ liệu (từ 26/26 trip bị rò rỉ khi split theo frame ngẫu nhiên giảm về 0 trip), đảm bảo mô hình Computer Vision học đúng tính khái quát hóa thay vì học vẹt cảnh quan.
- **Chưa rõ:** Độ trễ pha tối ưu khi đồng bộ telemetry tần số cao (10-50Hz) với khung hình tần số thấp (2 FPS) tại các thời điểm xe tăng tốc đột ngột hoặc đánh lái gắt; phép nội suy/join `asof` có thể gặp độ lệch nhỏ nếu đồng hồ phần cứng bị trôi phi tuyến tính trong hành trình dài.
- **Lỗi pipeline chưa bắt được:** Hiện tượng **chói loá cục bộ quá mức (extreme glare / overexposure)** do camera chiếu thẳng vào mặt trời hoặc đèn pha xe ngược chiều ban đêm làm trắng xoá toàn bộ góc nhìn trung tâm. Khung hình này vẫn có độ sáng cao (không phải khung đen) và vẫn có thể có độ tương phản cạnh cục bộ (vượt qua test blur), nhưng thông tin làn đường thực tế đã bị mất hoàn toàn, có thể gây nguy hiểm nếu đưa vào tập huấn luyện xe tự hành.
