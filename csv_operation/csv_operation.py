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
        return {merchant_id: data.get("消息模板") for merchant_id, data in merchant_site_dict.items()}

    def update_entity(self):
        """
        构建key为merchant_id，
        value为{
            'CS名字': '',
            'mcc': [],
            '主体-站点': {'主体': ['站点']},
            '主体详细信息': [],
            '适用法律国家': '',
            '消息模板': []
        }的字典

        Returns:

        """

        def convert_row_to_dict():
            entity_details_dict = {
                "SHIJIE TRADING LTD":
                    """
您好，您的以下站点分配主体有更新，请在2023-9-27（周三）18:00前完成主体更新，具体如下：
站点：

{}

分配主体：
Company Name: SHIJIE TRADING LTD
Company number:10377515
Registered office address:29-30Frith StreetLondonEnglandWD5LG
(This is NOT a returning address)

网修：
1.请修改terms和contact下的主体为SHIJIE TRADING LTD主体  
2.T&C - Governing Law 适用法律国家修改为UK

如果网站不再使用，需要报备关停哦。
                    """,
                "Mexong Ltd":
                    """
您好，您的以下站点分配主体有更新，请在2023-9-27（周三）18:00前完成主体更新，具体如下：
站点：

{}

分配主体：
Company Name:Mexong Ltd
Registration Number:12948246
CompanyAddress:309 Winston House 2 Dollis Park, London, England, N3 1HF
(this is not a returning address)

网修：
1.请修改terms和contact下的主体为 Kristal F.l. Societa'a Responsabilita' Limitata Semplificata主体  
2.T&C - Governing Law 适用法律国家修改为IT

如果网站不再使用，需要报备关停哦。
                    """,
                "KELI GROUP DI LIAO KELI":
                    """
您好，您的以下站点分配主体有更新，请在2023-9-27（周三）18:00前完成主体更新，具体如下：
站点：

{}

分配主体：
Company Name: KELI GROUP DI LIAO KELI
Registration Number: MB-2591094
Address: AGRATE BRIANZA (MB) VIA INDUSTRIE 86 CAP 20864
(THIS IS NOT A RETURNIG ADDRESS)

网修：
1.请修改terms和contact下的主体为KELI GROUP DI LIAO KELI主体  
2.T&C - Governing Law 适用法律国家修改为IT

如果网站不再使用，需要报备关停哦。
                    """,
                "KRISTAL F.I. SOCIETA' ARESPONSABILITA' LIMITATASEMPLIFICATA":
                    """
您好，您的以下站点分配主体有更新，请在2023-9-27（周三）18:00前完成主体更新，具体如下：
站点：

{}

分配主体：
Company Name: Kristal F.l. Societa'a Responsabilita' Limitata Semplificata
Registration Number:MI-2595919
Company Address: Milano (Ml) Via Tonale 12 CAP 20125
(this is not a returning address)

网修：
1.请修改terms和contact下的主体为Mexong Ltd主体  
2.T&C - Governing Law 适用法律国家修改为UK

如果网站不再使用，需要报备关停哦。
                    """
            }
            lines = 0
            merchant_site_dict = {}
            for row in self.read_csv_file():
                merchant_id = row[0]
                site = row[1]
                mcc = row[2]
                entity: str = row[3]
                country = row[4]
                cs = row[5]
                merchant_value = merchant_site_dict.setdefault(merchant_id, {'cs名字': cs})
                merchant_value.setdefault('主体-站点', {}).setdefault(entity, []).append(site)
                merchant_value.setdefault('mcc', []).append(mcc)
                merchant_value['主体详细信息'] = [entity_details_dict.get(entity, "") for entity in
                                                  merchant_value['主体-站点'].keys()]
                merchant_value['适用法律国家'] = country
                process_msg_template(merchant_site_dict)
                lines += 1
            return lines, merchant_site_dict

        def process_msg_template(merchant_site_dict):
            for merchant_id, data in merchant_site_dict.items():
                data['消息模板'] = []
                for index, entity in enumerate(data['主体-站点'].keys()):
                    sites = data['主体-站点'].get(entity, [])
                    matching_entity_details = data['主体详细信息'][index].format("\n".join(sites))
                    data['消息模板'].append(matching_entity_details)

        return convert_row_to_dict()


if __name__ == '__main__':
    # 通知下架主体
    # csv_operation = CsvOperation("C:\\Users\\1\\Desktop", 'test.csv')
    # count, merchant_data = csv_operation.convert_row_to_dict()
    # reduced_merchant_data = CsvOperation.reduced_merchant_site_dict(merchant_data)
    # print(f"一共处理了{count}记录")
    # print(f"处理过的商户信息如下：{merchant_data}")
    # print(f"精简后的商户信息如下：{reduced_merchant_data}")
    #
    # wx = WxOperation()
    # wx.send_msg_without_gui(reduced_merchant_data, file_paths=[], add_remark_name=False)

    # 通知更新主体
    csv_operation = CsvOperation("C:\\Users\\1\\Desktop", '新主体通知 - Copy.csv')
    count, merchant_data = csv_operation.update_entity()
    reduced_merchant_data = CsvOperation.reduced_merchant_site_dict(merchant_data)
    print(f"一共处理了{count}记录")
    # print(f"处理过的商户信息如下：{merchant_data}")
    print(f"精简后的商户信息如下：{reduced_merchant_data}")

    wx = WxOperation()
    wx.send_msg_without_gui(reduced_merchant_data, file_paths=[], add_remark_name=False)
