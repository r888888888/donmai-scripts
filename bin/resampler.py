#! .virtualenv/bin/python

from dotenv import load_dotenv, find_dotenv
import os
import boto3
import time
import requests
import tempfile
from jpegtran import JPEGImage
from shutil import copyfile
from contextlib import closing
from PIL import Image
from subprocess import call
import functools
import hashlib
import signal

load_dotenv(find_dotenv())

def pure_pil_alpha_to_color_v2(image, color=(255, 255, 255)):
  """Alpha composite an RGBA Image with a specified color.

  Simpler, faster version than the solutions above.

  Source: http://stackoverflow.com/a/9459208/284318

  Keyword Arguments:
  image -- PIL RGBA Image object
  color -- Tuple r, g, b (default 255, 255, 255)

  """
  image.load()  # needed for split()
  channels = image.split()
  if len(channels) < 4:
    return image
  background = Image.new('RGB', image.size, color)
  background.paste(image, mask=channels[3])  # 3 is the alpha channel
  image.close()
  return background

def scale_dim(w, h, max_size):
  """Scale a tuple such that it fits within a maximum size.

  Parameters
  ----------
  w : int
      Width

  h : int
      Height

  max_size : int
      The maximum width or height. `w` and `h` will be scaled such
      that neither is larger than `max_size`, maintaining the
      aspect ratio.

  Returns
  -------
  (int, int)
      The scaled width and height returned as a tuple.
  """
  if w > h:
    ratio = float(max_size) / float(w)
  else:
    ratio = float(max_size) / float(h)

  return (int(ratio * w), int(ratio * h))

def download_image(url):
  """Downloads an image to a temporary file.

  Parameters
  ----------
  url : string
      A URL pointing to a file.

  Returns
  -------
  file
      A file pointing to the downloaded resource. The caller is 
      responsible for closing it.
  """
  ext = os.path.splitext(url)[1].lower()
  file = tempfile.NamedTemporaryFile("w+b", suffix=ext)
  with closing(requests.get(url, stream=True)) as resp:
    for chunk in resp.iter_content(chunk_size=None):
      if chunk:
        file.write(chunk)
  return file

def resize_jpg(file, n):
  """Resize a JPEG image.

  Parameters
  ----------
  file : file
      The original JPEG image resource.

  n : int
      The maximum width/height of the resized image.

  Returns
  -------
  file or None
      A file pointing to the resized image in JPEG format, or None if 
      the file did not require resizing based on `n`. The caller is 
      responsible for closing it.
  """
  output = tempfile.NamedTemporaryFile("w+b", suffix=".jpg")
  img = JPEGImage(file.name)
  if img.width > n or img.height > n:
    w, h = scale_dim(img.width, img.height, n)
    img.downscale(w, h, 90).save(output.name)
    return output
  else:
    return None

def resize_general(file, n):
  """Resize an image.

  Parameters
  ----------
  file : file
      The original JPEG image resource.

  n : int
      The maximum width/height of the resized image.

  Returns
  -------
  file or None
      A file pointing to the resized image in JPEG format, or None if 
      the file did not require resizing based on `n`. The caller is 
      responsible for closing it.
  """
  output = tempfile.NamedTemporaryFile("w+b", suffix=".jpg")
  img = Image.open(file.name)
  if img.mode != "RGB":
    rgb = img.convert("RGB")
    img.close()
    img = rgb
  img = pure_pil_alpha_to_color_v2(img)
  if img.width > n or img.height > n:
    w, h = scale_dim(img.width, img.height, n)
    img = img.resize((w, h), resample=Image.LANCZOS)
    img.save(output.name, 'JPEG', quality=90)
    img.close()
    return output
  else:
    return None

def generate(url, original, n):
  """Resize and optimize an image.

  Parameters
  ----------
  url : string
      The original URL source of the file. This is only used to determine
      the original content type and is otherwise unused.

  original : file
      A file pointing to the original image resource.

  n : int
      The maximum width/height of the resized image.

  Returns
  -------
  (int, file) or None
      Either a tuple of the resized size and file of the resized image
      in JPEG format, or None if no resize was performed. The caller
      is responsible for closing the file.
  """

  ext = os.path.splitext(url)[1].lower()
  if ext == ".jpg":
    resample = resize_jpg(original, n)
  elif ext in [".png", ".gif"]:
    resample = resize_general(original, n)

  if resample is not None:
    optimized = tempfile.NamedTemporaryFile("w+b", suffix=".jpg")
    call(["guetzli", "--quality", "90", resample.name, optimized.name])
    resample.close()
    return (n, optimized)
  else:
    return None

def download_and_generate(url):
  """Download, resize, and optimize an image.

  Parameters
  ----------
  url : string
      The original URL source of the file.

  Returns
  -------
  [(int, string)]
      A list of tuples. Each tuple contains the size of the resized image,
      and a path to the resized image on the file system.
  """

  widths = [150, 850]
  original = download_image(url)
  paths = list(map(functools.partial(generate, url, original), widths))
  original.close()
  return paths

def upload(md5, local_path, remote_path):
  """Upload a file to a remote server.

  Parameters
  ----------
  md5 : string
      MD5 key of file (used for debugging and logging only)

  local_path : string
      Path to a file on the local file system.

  remote_path : string
      Path to the file destination on the remote server.
  """
  size = os.stat(local_path).st_size
  print("upload s3 size={} md5={}".format(size, md5))
  servers = os.environ.get("DANBOORU_SERVERS").split(",")
  f = lambda x: call(["scp", local_path, x + ":" + remote_path])
  list(map(f, servers))

def upload_s3(md5, local_path, remote_name):
  """Upload a file to S3.

  Parameters
  ----------
  md5 : string
      MD5 key of file (used for debugging and logging only)

  local_path : string
      Path to a file on the local file system.

  remote_name : string
      The file name to use on the S3 server.
  """
  s3 = boto3.client("s3")
  key = "sample/" + remote_name
  with closing(open(local_path, "rb")) as file:
    size = os.stat(local_path).st_size
    resize_md5 = hashlib.md5(file.read()).hexdigest()
    file.seek(0)
    print("upload s3 size={} md5={}".format(size, md5))
    s3.put_object(ACL="public-read", Body=file, Bucket="danbooru", Key=key, ContentMD5=resize_md5)

def process_queue():
  """Listen to the SQS queue and process messages.
  """
  queue_url = os.environ.get("AWS_SQS_URL")
  sqs = boto3.client("sqs")
  loop = True

  while loop:
    try:
      response = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=20)

      if "Messages" in response:
        for message in response["Messages"]:
          receipt_handle = message["ReceiptHandle"]
          md5, image_url = message["Body"].split(",")
          paths = list(filter(None.__ne__, download_and_generate(image_url)))
          for size, file in paths:
            if size == 150:
              upload(md5, file.name, "/var/www/danbooru2/shared/data/preview/" + md5 + ".jpg")
            else:
              upload_s3(md5, file.name, "sample-" + md5 + ".jpg")
          sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

      time.sleep(1)

    except KeyboardInterrupt:
      loop = False
      print("exiting")

process_queue()
