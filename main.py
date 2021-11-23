import boto3
import sys
import logging

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

bucket_name = sys.argv[1]
operation_name = sys.argv[2]
if len(sys.argv) > 3:
    object_key = sys.argv[3]
    data = sys.argv[4]

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)


def list_objects(prefix=None):
    """
    Lists the objects in a bucket, optionally filtered by a prefix.

    Usage is shown in usage_demo at the end of this module.

    :param bucket: The bucket to query.
    :param prefix: When specified, only objects that start with this prefix are listed.
    :return: The list of objects.
    """
    try:
        if not prefix:
            objects = list(bucket.objects.all())
        else:
            objects = list(bucket.objects.filter(Prefix=prefix))
        logger.info("Got objects %s from bucket '%s'", [
                    o.key for o in objects], bucket.name)
        [print(o.key) for o in objects]

    except ClientError:
        logger.exception("Couldn't get objects for bucket '%s'.", bucket.name)
        raise
    else:
        return objects


def put_object(object_key, data):
    """
    Upload data to a bucket and identify it with the specified object key.

    Usage is shown in usage_demo at the end of this module.

    :param bucket: The bucket to receive the data.
    :param object_key: The key of the object in the bucket.
    :param data: The data to upload. This can either be bytes or a string. When this
                 argument is a string, it is interpreted as a file name, which is
                 opened in read bytes mode.
    """
    put_data = data
    if isinstance(data, str):
        try:
            put_data = open(data, 'rb')
        except IOError:
            logger.exception(
                "Expected file name or binary data, got '%s'.", data)
            raise

    try:
        obj = bucket.Object(object_key)
        obj.put(Body=put_data)
        obj.wait_until_exists()
        logger.info("Put object '%s' to bucket '%s'.", object_key, bucket.name)
        print(object_key, "uploaded successfully")
    except ClientError:
        logger.exception("Couldn't put object '%s' to bucket '%s'.",
                         object_key, bucket.name)
        raise
    finally:
        if getattr(put_data, 'close', None):
            put_data.close()


def main():
    if operation_name == 'list_objects':
        list_objects()

    if operation_name == 'put_object':
        put_object(object_key, data)


if __name__ == "__main__":
    main()
