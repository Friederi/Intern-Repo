import json
import pandas as pd
import io
import boto3
from lxml import etree
import logging
import botocore

s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        input_bucket = event['input_bucket']
        input_csv_key = event['input_csv_key']
        input_mapping_key = event['input_mapping_key']
        output_bucket = event['output_bucket']
        output_prefix = event.get('output_prefix', 'output/')

        # Load CSV from S3
        try:
            csv_obj = s3.get_object(Bucket=input_bucket, Key=input_csv_key)
        except botocore.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                logger.error(f"404 - File Not Found: '{input_csv_key}' not found in bucket '{input_bucket}'")
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        error_code: {
                            "type": "Missing Artifacts",
                            "message": "No artifacts found for template"
                        },
                    })
                }
            else:
                logger.error(f"500 - S3 Access Failure (CSV): {str(e)}")
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        error_code: {
                            "type": "S3 Access Failure",
                            "message": "Log error, notify admin"
                        },
                    })
                }

        # Read CSV into DataFrame
        try:
            df = pd.read_csv(io.BytesIO(csv_obj['Body'].read()), dtype=str).fillna("")
        except Exception as e:
            logger.error(f"400 - Invalid CSV file: {str(e)}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    "error": {
                        "type": "Invalid CSV",
                        "message": "CSV file is not valid"
                    },
                })
            }

        # Load Mapping JSON from S3
        try:
            mapping_obj = s3.get_object(Bucket=input_bucket, Key=input_mapping_key)
        except botocore.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                logger.error(f"404 - File Not Found: '{input_mapping_key}' not found in bucket '{input_bucket}'")
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        error_code: {
                            "type": "Missing Artifacts",
                            "message": "No artifacts found for template"
                        },
                    })
                }
            else:
                logger.error(f"500 - S3 Access Failure (mapping JSON): {str(e)}")
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        error_code: {
                            "type": "S3 Access Failure",
                            "message": "Log error, notify admin"
                        },
                    })
                }
        
        # Read mapping JSON into a dictionary
        try:
            mapping = json.loads(mapping_obj['Body'].read().decode('utf-8'))
        except json.JSONDecodeError as e:
            logger.error(f"400 - Invalid JSON file: {str(e)}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    "error": {
                        "type": "Invalid JSON",
                        "message": "Mapping Template is not valid"
                    },
                })
            }
        
        # Create intermediate mapped DataFrame
        mapped_data = []

        # Collect all possible XML keys from the mapping
        all_xml_keys = set()
        for csv_key, xml_key in mapping.items():
            if isinstance(xml_key, dict):
                all_xml_keys.update(xml_key.values())
            else:
                all_xml_keys.add(xml_key)

        for _, row in df.iterrows():
            mapped_row = {key: "" for key in all_xml_keys}  # Initialize all keys with empty strings
            for csv_key, xml_key in mapping.items():
                if isinstance(xml_key, dict):
                    for nested_key, nested_value in xml_key.items():
                        if nested_key in row.get(csv_key, ""):
                            mapped_row[nested_value] = row[csv_key]
                else:
                    if csv_key in row:
                        mapped_row[xml_key] = row.get(csv_key, "")
            mapped_data.append(mapped_row)


        print(f"Mapped data: {mapped_data}")
        mapped_df = pd.DataFrame(mapped_data)
        mapped_df.head()

        # Save intermediate CSV to memory
        csv_buffer = io.StringIO()
        mapped_df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
        s3.put_object(Bucket=output_bucket, Key=f"{output_prefix}intermediate_output.csv", Body=csv_buffer.getvalue())

        # Create XML
        root = etree.Element("ddash")

        for data in mapped_data:
            # level 1
            order_elem = etree.SubElement(root, "order_template_attributes")

            order_tags = [
                "order_id", "order_previous_id", "order_accept_number",
                "order_accept_datetime", "order_customer_master_id", "order_customer_name",
                "order_customer_zip_code", "order_customer_phone_number",
                "order_sales_user_department", "order_sales_user_name",
                "order_subtotal_amount", "order_estimation_number", "order_memo"
            ]
            for tag in order_tags:
                if tag in data:
                    etree.SubElement(order_elem, tag).text = data[tag]

            # level 2
            product_elem = etree.SubElement(order_elem, "product_template_attributes")
            product_tags = [
                "product_id", "product_management_user_id", "product_management_user_name",
                "product_name", "product_name_kana", "product_master_id",
                "product_final_size_master_id", "product_final_size_master_name",
                "product_vertical_final_size_mm", "product_horizontal_final_size_mm",
                "product_weight_g", "product_quantities", "product_ordered_quantities",
                "product_external_aux_quantities", "product_internal_aux_quantities",
                "product_postpress_type_name", "product_extended_size_master_id",
                "product_extended_size_master_name", "product_vertical_extended_size",
                "product_horizontal_extended_size"
            ]
            for tag in product_tags:
                if tag in data:
                    etree.SubElement(product_elem, tag).text = data[tag]

            # level 3
            delivery_in_product_elem = etree.SubElement(product_elem, "product_delivery_template_attributes")
            for tag in ["delivery_id", "product_delivery_quantities"]:
                if tag in data:
                    etree.SubElement(delivery_in_product_elem, tag).text = data[tag]

            part_elem = etree.SubElement(product_elem, "part_template_attributes")
            part_tags = [
                "part_seq", "part_type_id", "part_type_name", "part_original_file_path",
                "part_original_data_pages", "part_quantities", "part_external_aux_quantities",
                "part_internal_aux_quantities", "part_final_size_master_id",
                "part_final_size_master_name", "part_media_master_name",
                "part_colorant_front_number", "part_colorant_back_number", "part_memo",
                "platemaking_process_template_attributes", "prepress_process_template_attributes",
                "print_process_template_attributes", "postpress_process_template_attributes"
            ]
            for tag in part_tags:
                if tag in data:
                    etree.SubElement(part_elem, tag).text = data[tag]

            # level 2
            delivery_elem = etree.SubElement(order_elem, "delivery_template_attributes")
            delivery_tags = [
                "delivery_seq", "delivery_id", "delivery_planned_shipping_start_datetime",
                "delivery_planned_shipping_end_datetime", "delivery_planned_delivery_datetime",
                "delivery_customer_name", "delivery_zip_code", "delivery_phone_number",
                "delivery_supplier_master_id", "delivery_supplier_master_name",
                "delivery_case_quantities", "delivery_packing_type_master_id",
                "delivery_packing_type_master_name", "delivery_destination",
                "delivery_requester", "delivery_memo"
            ]
            for tag in delivery_tags:
                if tag in data:
                    etree.SubElement(delivery_elem, tag).text = data[tag]

        # Save final XML to memory and upload to S3
        xml_buffer = io.BytesIO()
        tree = etree.ElementTree(root)
        tree.write(xml_buffer, pretty_print=True, xml_declaration=True, encoding="UTF-8")
        s3.put_object(Bucket=output_bucket, Key=f"{output_prefix}output.xml", Body=xml_buffer.getvalue())

        # return the response with the mapped DataFrame
        return {
        'statusCode': 200,
        'body': json.dumps(
            {
            "message": "Success",
            "status_code": 200,
            "response_body": 
            {
                "previous_order_id": "",
                "product_name": "",
                "customer_name": "",
                "product_finished_size_name": "",
                "order_timestamp_start": "",
                "order_timestamp_end": "",
                "product_template_type": ""
            },
            "records": mapped_df.to_dict(orient='records')
            }, ensure_ascii=False)  
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
