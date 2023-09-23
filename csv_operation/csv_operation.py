import csv
import os

from wechat_operation.wx_operation import WxOperation


class CsvOperation:
    reserved_key = "是否KA/保留商户"

    def __init__(self, directory: str, filename: str) -> None:
        if not directory or not filename:
            raise ValueError("Both directory and filename cannot be empty.")
        self.directory = directory
        self.filename = filename
        self.fullpath = os.path.join(directory, filename)

    def read_csv_file(self):
        with open(self.fullpath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                yield row

    def convert_row_to_dict(self):
        lines = 0
        merchant_site_dict = {}
        for row in self.read_csv_file():
            lines += 1
            merchant_id = row[0]
            site = row[1]
            name = row[2]
            reserved = row[3] == 'KA' or row[4] == '是'
            merchant_value = merchant_site_dict.setdefault(merchant_id, {'cs名字': name})
            merchant_value.setdefault('站点', []).append(site)
            merchant_value[self.reserved_key] = '是' if reserved else '否'
        self.process_msg_template(merchant_site_dict)
        return lines, merchant_site_dict

    def process_msg_template(self, merchant_site_dict):
        # 保留商户消息模板
        reserved_msg_template = """
您好，应合规优化需要，之前分配的主体信息不再使用，下周会为您提供新的主体信息，以下站点contact页面和T&C页面需要删除旧的主体信息。
{}
更换主体信息期间不影响正常交易，如果站点不再使用，请报备关停并删除主体信息噢
请在9-23（周六）12:00前完成整改，若站点已删除旧主体信息或未使用旧主体信息，请忽略此通知。

有疑问请找CS-{}
        """
        # 非保留商户消息模板
        normal_msg_template = """
您好，应合规要求，分配的主体信息暂时不提供使用，您的以下站点需要删除我们的主体信息内容。
{}
如果站点不再使用，请报备关停并删除主体信息噢							
请在9-23（周六）18:00前完成整改，若站点已删除提供的主体信息，请忽略此通知。

有疑问请找CS-{}
        """

        for merchant_id, data in merchant_site_dict.items():
            data['消息模板'] = [
                reserved_msg_template.format("\n".join(data['站点']), data['cs名字']) if data[self.reserved_key] == '是'
                else normal_msg_template.format("\n".join(data['站点']), data['cs名字'])]

    @staticmethod
    def reduced_merchant_site_dict(merchant_site_dict):
        return {merchant_id: data.get('消息模板') for merchant_id, data in merchant_site_dict.items()}


if __name__ == '__main__':
    csv_operation = CsvOperation("C:\\Users\\1\\Desktop", 'test.csv')
    count, merchant_data = csv_operation.convert_row_to_dict()
    reduced_merchant_data = CsvOperation.reduced_merchant_site_dict(merchant_data)
    print(f"一共处理了{count}记录")
    print(f"处理过的商户信息如下：{merchant_data}")
    print(f"精简后的商户信息如下：{reduced_merchant_data}")

    wx = WxOperation()
    wx.send_msg_without_gui(reduced_merchant_data, file_paths=[], add_remark_name=False)
