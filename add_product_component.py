
from flask import Flask, request, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine
import pandas as pd
from io import BytesIO
import pymysql
import requests

engine = None
app = Flask(__name__)
# Cấu hình CORS cho ứng dụng Flask
# supports_credentials=True nếu ko có thì sẽ không gửi được cookie từ frontend sang backend
# tuy mình ko xài cookie nhưng api mình gửi lên có cookie để dùng cho spring boot
CORS(app, supports_credentials=True)

# 1. khi người dùng upload file lên thì sẽ gửi request lên backend springBoot (verify_token)
# để xác thực xem có phải là admin hay không
# 2. nếu xác thực thành công thì mới cho phép upload file lên

# URL của endpoint Spring Boot dùng để xác thực người dùng trước khi upload file
AUTH_URL = "http://localhost:8080/api/admin/addComponentProductFile"
def verify_token(token):
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(AUTH_URL, headers=headers)
        if response.status_code == 200 and response.json().get("status") == "success":
            return True
        return False
    except requests.RequestException:
        return False

def init_database():
    # Trong Python, khi gán giá trị cho biến trong hàm, mặc định nó sẽ tạo biến cục bộ
    # Dùng global để chỉ định muốn thay đổi biến toàn cục đã tồn tại
    global engine
    engine = create_engine('mysql+pymysql://root:29092003@localhost/websitepc')
    print("Đã kết nối với database")


@app.route("/api/admin/addComponentFile", methods=["POST"])
def add_component():
    try:
        # print("request:", request.files)

        token = request.headers.get('Authorization')
        if not token or not token.startswith("Bearer "):
            return jsonify({"error": "Missing or invalid token"}), 401

        token = token.replace("Bearer ", "")

        # print("token:", token)

        # Xác thực token qua Spring Boot
        if not verify_token(token):
            return jsonify({"error": "Unauthorized access"}), 401

        # Kiểm tra file nếu không có file nào được gửi lên trả về lỗi
        if 'file' not in request.files:
            return {"error": "Không tìm thấy file"}, 400

        # nhận file từ request với key 'file'
        file = request.files['file']
        # print("file.filename:", file.filename)
        # nếu file không có tên (ví dụ: người dùng không chọn file nào) trả về lỗi
        if file.filename == '':
            return {"error": "Chưa chọn file"}, 400

        # file.read(): Lấy nội dung file upload dưới dạng bytes.
        # Tác dụng của BytesIO: Chuyển bytes thành một luồng dữ liệu (stream)
        # để pd.read_excel đọc file Excel mà không cần lưu file, nếu ko có BytesIO thì read_excel sẽ bị lỗi
        # Tác dụng: Chỉ định kiểu dữ liệu cho cột component_active là chuỗi (str) khi đọc file Excel.
        # Mặc định, Pandas tự động suy ra kiểu dữ liệu, có thể hiểu nhầm true/false là boolean thay vì chuỗi.
        df = pd.read_excel(
                BytesIO(file.read()), dtype={
                    'component_active': str, 'component_name': str, 'component_type': str
                }
            )

            # Kiểm tra cột cần thiết nếu không có cột nào trong DataFrame trả về lỗi
        if (
                'component_name' not in df.columns
                or 'component_type' not in df.columns
                or 'component_active' not in df.columns
        ):
            return {"error": "File Excel phải chứa cột 'component_name' và 'component_type' và 'component_active'"}, 400

        # Chuyển component_active thành chuỗi thường
        df['component_active'] = df['component_active'].str.lower()

        # 1. df[['component_name', 'component_type', 'component_active']]:
        # Lấy một tập hợp con của DataFrame df, chỉ bao gồm ba cột: component_name, component_type, và component_active.
        # Kết quả là một DataFrame mới chỉ chứa các cột này, loại bỏ các cột khác (nếu có) trong df.
        #
        # 2. .to_sql('component', engine, if_exists='append', index=False):
        # .to_sql: Phương thức của Pandas để đẩy dữ liệu từ DataFrame vào một bảng trong database.
        # 'component': Tên bảng trong database (ở đây là bảng component) mà dữ liệu sẽ được thêm vào.
        # engine: Đối tượng kết nối database (tạo bởi SQLAlchemy.create_engine), dùng để giao tiếp với MySQL.
        # if_exists='append': Nếu bảng component đã tồn tại, dữ liệu sẽ được thêm (append) vào cuối bảng.
        # Các tùy chọn khác là fail (báo lỗi nếu bảng tồn tại) hoặc replace (xóa bảng cũ và tạo mới).
        # index=False: Không đưa cột chỉ số (index) của DataFrame vào bảng database.
        # Nếu index=True, cột chỉ số sẽ được thêm dưới dạng một cột (thường tên là index).
        (df[['component_name', 'component_type', 'component_active']]
         .to_sql('component', engine, if_exists='append', index=False))

        return {"status": 200}, 200

    except Exception as e:
        return {"error": str(e)}, 500

@app.route("/api/admin/addProductFile", methods=["POST"])
def add_product():
    try:
        # print("request:", request.files)
        #ở đây trước khi thực hiện thì mình sẽ gửi request lên backend springBoot
        # để xác thực xem có phải là admin hay không (chỉ cần tạo 1 route để xác thực thôi)

        token = request.headers.get('Authorization')
        if not token or not token.startswith("Bearer "):
            return jsonify({"error": "Missing or invalid token"}), 401

        token = token.replace("Bearer ", "")

        # print("token:", token)

        # Xác thực token qua Spring Boot
        if not verify_token(token):
            return jsonify({"error": "Unauthorized access"}), 401

        # Kiểm tra file nếu không có file nào được gửi lên trả về lỗi
        if 'file' not in request.files:
            return {"error": "Không tìm thấy file"}, 400

        # nhận file từ request với key 'file'
        file = request.files['file']
        # print("file.filename:", file.filename)
        # nếu file không có tên (ví dụ: người dùng không chọn file nào) trả về lỗi
        if file.filename == '':
            return {"error": "Chưa chọn file"}, 400

        # Đọc file Excel, ép kiểu cho componentIds và imgLinks
        df = pd.read_excel(BytesIO(file.read()), dtype={
                'product_in_stock': int,
                'product_name': str,
                'create_date': str,
                'product_active': str,
                'product_price': int,
                'product_type': str,
                'component_ids': str,
                'img_links': str
        })

        # Kiểm tra cột cần thiết
        # Lặp qua từng phần tử trong required_columns (for col in required_columns):
        # Python lấy từng phần tử col từ danh sách required_columns.
        # Với mỗi col, nó kiểm tra col in df.columns.
        #
        # Kiểm tra điều kiện col in df.columns:
        # Nếu col có trong df.columns, trả về True.
        # Nếu col không có trong df.columns, trả về False.
        #
        # Hàm all():
        # Hàm all() yêu cầu tất cả các giá trị trong generator
        # (tức là kết quả của col in df.columns) phải là True để trả về True.
        # Nếu bất kỳ giá trị nào là False, all() ngay lập tức trả về False
        # mà không cần kiểm tra các phần tử tiếp theo
        #
        # Toàn bộ biểu thức if not ...:
        # Nếu all(...) trả về False, thì not all(...) sẽ là True
        # Nếu all(...) trả về True, thì not all(...) sẽ là False
        required_columns = ['product_in_stock', 'product_name', 'product_price',
                            'product_active','product_type', 'component_ids', 'img_links',
                            'create_date']
        if not all(col in df.columns for col in required_columns):
            return {"error": "File Excel phải chứa các cột: " + ", ".join(required_columns)}, 400

        # Chuyển product_active thành chuỗi thường
        df['product_active'] = df['product_active'].str.lower()

        # Kết nối database
        # charset='utf8mb4': Sử dụng mã hóa UTF-8 để hỗ trợ ký tự Unicode.
        # cursorclass=pymysql.cursors.DictCursor:
        # Con trỏ trả về kết quả dưới dạng list từ điển (mỗi hàng là một dict với tên cột làm khóa).
        # ví dụ: [{'product_id': 128}, {'product_id': 127}]
        connection = pymysql.connect(
                host='localhost',
                user='root',
                password='29092003',
                database='websitepc',
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
        )

        # Khối try xử lý ngoại lệ để đảm bảo an toàn khi làm việc với cơ sở dữ liệu.
        # connection.cursor() tạo một con trỏ để thực thi các câu lệnh SQL.
        # with ... as cursor: Đảm bảo con trỏ được đóng tự động sau khi sử dụng,
        # ngay cả khi có lỗi xảy ra.
        try:
            with connection.cursor() as cursor:

                print("df", df)

                # lấy các cột cần thiết từ DataFrame df gán cho df_products
                df_products = df[['product_in_stock', 'product_name',
                                  'product_price', 'product_type',
                                  'product_active', 'create_date']]
                # chunksize=1000 chia dữ liệu thành các lô (chunk) nhỏ, mỗi lô chứa tối đa 1000 hàng.
                # chia nhỏ dữ liệu giúp giảm tải bộ nhớ và tăng hiệu suất khi chèn dữ liệu lớn vào database.
                df_products.to_sql('product', engine, if_exists='append', index=False, chunksize=1000)


                # Trong Python, khi sử dụng cursor.execute() với thư viện như pymysql, tham số được truyền vào câu SQL
                # thông qua một tuple hoặc list. Điều này là yêu cầu của API cơ sở dữ liệu Python (DB-API)
                # để đảm bảo an toàn và ngăn chặn SQL injection.

                # Lấy product_id của các sản phẩm vừa thêm (bằng sắp xếp theo thứ tự giảm dần và giới hạn số lượng
                # bằng số dòng trong DataFrame)
                # Sử dụng %s và tuple (len(df),) giúp ngăn chặn SQL injection, đảm bảo an toàn.
                # Dấu phẩy , trong (len(df),) là bắt buộc để Python hiểu đây là một tuple, không phải một biểu thức đơn giản
                # Trong (len(df),), không có tham số thứ hai, cũng không có giá trị null. Tuple này chỉ có một phần tử là len(df)
                cursor.execute("SELECT product_id FROM product ORDER BY product_id DESC LIMIT %s", (len(df),))

                # Truy vấn SELECT không thay đổi dữ liệu mà chỉ đọc dữ liệu từ cơ sở dữ liệu.
                # Kết quả của truy vấn được lưu tạm trong con trỏ (cursor).
                # fetchall() lấy toàn bộ kết quả từ con trỏ và đưa vào bộ nhớ
                # Nếu không gọi fetchall() (hoặc các phương thức tương tự như fetchone(), fetchmany()),
                # sẽ không truy cập được dữ liệu trả về từ truy vấn.
                fetched_data = cursor.fetchall()
                # 1 list dict
                print("cursor.fetchall()", fetched_data)

                # lặp qua từng dict trong fetched_data và mỗi dict lấy ra giá trị của khóa 'product_id'
                product_ids = [row['product_id'] for row in fetched_data]

                # Đảo ngược để khớp với thứ tự trong DataFrame (ví dụ khi thêm vào 2 sp mới thì product_ids sẽ là [2, 1]
                # theo câu sql trên, nhưng trong df thì thứ tự là [1, 2])
                product_ids.reverse()
                print("product_ids", product_ids)

                # Thêm vào bảng product_component ,tới đây
                # Khởi tạo một danh sách rỗng component_data để lưu các cặp (product_id, component_id)
                component_data = []
                # idx: Chỉ số hàng (index).
                # row: Đối tượng đại diện cho hàng, chứa dữ liệu các cột.
                for idx, row in df.iterrows():
                    # Lấy product_id tương ứng với hàng hiện tại từ product_ids
                    # (product_ids là một danh sách hoặc Series có cùng thứ tự với df).
                    product_id = product_ids[idx]
                    # Lấy giá trị cột component_ids của hàng hiện tại (ví dụ: "1,2,3").
                    # Dùng split(',') để tách chuỗi thành danh sách các chuỗi con, ví dụ: ["1", "2", "3"].
                    component_ids = row['component_ids'].split(',')
                    # Lặp qua từng component_id trong danh sách component_ids.
                    for component_id in component_ids:
                        # Tạo một tuple (product_id, component_id)
                        # component_id.strip(): Loại bỏ khoảng trắng (nếu có) trong chuỗi component_id.
                        component_data.append((product_id, int(component_id.strip())))

                print("component_data", component_data)

                # nếu component_data không rỗng thì thực hiện chèn dữ liệu vào bảng product_component
                if component_data:
                    cursor.executemany(
                        "INSERT INTO product_component (product_id, component_id) VALUES (%s, %s)",
                        component_data
                    )

                # Thêm vào bảng product_images
                # thuật toán tương tự component_data
                image_data = []
                for idx, row in df.iterrows():
                    product_id = product_ids[idx]
                    img_links = row['img_links'].split(',')
                    for img_link in img_links:
                        image_data.append((product_id, img_link.strip()))

                print("image_data", image_data)
                if image_data:
                    cursor.executemany(
                        "INSERT INTO img (product_id, img_link) VALUES (%s, %s)",
                        image_data
                        )

                # connection.commit() chỉ cần thiết khi bạn thực thi các câu
                # lệnh thay đổi dữ liệu như INSERT, UPDATE, DELETE
                # Nó đảm bảo các thay đổi được lưu vĩnh viễn vào cơ sở dữ liệu.
                # Nếu không gọi commit(), các thay đổi sẽ bị hủy khi kết nối đóng.
                connection.commit()

        finally:
            # đóng con trỏ để giải phóng tài nguyên
            connection.close()
        return {"status": 200}, 200

    except Exception as e:
        return {"error": str(e)}, 500

if __name__ == "__main__":
    init_database()
    # Khởi động server Flask (mặc định port 5000)
    app.run(port=5000, debug=True)