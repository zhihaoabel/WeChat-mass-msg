import csv
import os.path

from wechat_operation.wx_operation import WxOperation


class CsvOperation(object):
    reserved_key = "是否KA/保留商户"

    def __init__(self, directory, filename):
        """
        Args:
            directory (str): 文件目录
            filename (str): 文件名

        Raises:
            AssertionError: If either `directory` or `filename` is empty.

        Attributes:
            directory (str): The base path of the file.
            filename (str): The name of the file.

        """
        assert directory, "文件路径不能为空"
        assert filename, "文件名不能为空"

        self.filename = filename
        self.directory = directory
        self.fullpath = os.path.join(directory, filename)

    def read_csv_file(self) -> list:
        """
        读取csv文件

        Returns:
            A generator that yields each row of the CSV file as a list of values.

        Raises:
            FileNotFoundError: If the file specified by `fullpath` does not exist.
            IOError: If there is an error opening the file.
            Exception: If an unexpected error occurs.
        """
        try:
            with open(self.fullpath, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    yield row
        except FileNotFoundError:
            print("{} 路径下不存在该文件".format(self.fullpath))
        except IOError:
            print("打开文件出错")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def convert_row_2_dict(self):
        """
        Converts rows from a CSV file into a dictionary.

        Parameters:
        None

        Returns:
        A tuple containing the number of data lines and a dictionary of merchant data. The dictionary has merchant IDs as keys and values that include the name, sites, and whether the merchant is KA/retained.

        Example Usage:
        csv_operation = CsvOperation()
        lines, merchant_data = csv_operation.convert_row_2_dict()
        """
        # 记录数据条数
        lines = 0
        # key是商户号，value是站点列表
        merchant_site_dict = {}

        for row in self.read_csv_file():
            lines += 1
            # 每行数据第一列是商户号
            merchant_id = row[0]
            # 每行数据第二列是沾点
            site = row[1]
            # 每行数据第三列是cs名字
            name = row[2]
            # 每行数据第四列是否是KA， 每行数据第五列是否保留客户
            reserved = row[3] == 'KA' or row[4] == '是'

            # 构建dict
            if merchant_id not in merchant_site_dict:
                merchant_site_dict[merchant_id] = {'cs名字': name}
                merchant_site_dict.get(merchant_id)['站点'] = [site]
                merchant_site_dict.get(merchant_id)[CsvOperation.reserved_key] = '是' if reserved else '否'
            else:
                merchant_site_dict.get(merchant_id)['站点'].append(site)

        return lines, merchant_site_dict

    def process_msg_template(self, merchant_site_dict):
        # 保留商户消息模板
        reserved_msg_template = """
您好，应合规优化需要，之前分配的主体信息不再使用，下周会为您提供新的主体信息，以下站点contact页面和T&C页面需要删除旧的主体信息。
{}
更换主体信息期间不影响正常交易，如果站点不再使用，请报备关停并删除主体信息噢
请在9-23（周六）12:00前完成整改，若站点已删除旧主体信息或未使用旧主体信息，请忽略此通知。
        """
        # 非保留商户消息模板
        normal_msg_template = """
您好，应合规要求，分配的主体信息暂时不提供使用，您的以下站点需要删除我们的主体信息内容。
{}
如果站点不再使用，请报备关停并删除主体信息噢							
请在9-23（周六）18:00前完成整改，若站点已删除提供的主体信息，请忽略此通知。
        """
        msg = ''

        for merchant_id in merchant_site_dict:
            sites = "\n".join(merchant_site_dict[merchant_id].get('站点'))
            if merchant_site_dict[merchant_id].get(CsvOperation.reserved_key) == '是':
                msg = reserved_msg_template.format(sites)
            elif merchant_site_dict[merchant_id].get(CsvOperation.reserved_key) == '否':
                msg = normal_msg_template.format(sites)
            merchant_site_dict[merchant_id]['消息模板'] = [msg]

    def reduced_merchant_site_dict(self, merchant_site_dict):
        """
        从商户信息字典中去除不需要的信息
        """
        reduced_merchant_site_dict = {}
        for merchant_id in merchant_site_dict:
            reduced_merchant_site_dict[merchant_id] = merchant_site_dict[merchant_id].get('消息模板')
        return reduced_merchant_site_dict


# 测试用例
op = CsvOperation("C:\\Users\\1\\Desktop", 'test.csv')
print(op.fullpath)
count, records_dict = op.convert_row_2_dict()
op.process_msg_template(records_dict)
reduced_merchant_site_dict = op.reduced_merchant_site_dict(records_dict)
print("一共处理了{}记录".format(count))
print("处理过的商户信息如下：{}".format(records_dict))
print("精简后的商户信息如下：{}".format(reduced_merchant_site_dict))

wx = WxOperation()
wx.send_msg_without_gui(op.reduced_merchant_site_dict(records_dict), file_paths=[], add_remark_name=False)
