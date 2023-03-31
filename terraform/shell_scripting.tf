resource "null_resource" "install_dependencies" {
    provisioner "local-exec" {
        command = <<-EOT
            pip install pg8000 -t ./../data/src_extract
            pip install python-dotenv -t ./../data/src_extract
            pip install python-dotenv -t ./../data/src_load
            EOT
    }

    triggers = {
        dependencies_versions = filemd5("./../requirements.txt")
    }
}


resource "null_resource" "copy_src" {
    provisioner "local-exec" {
        # cp will not work if folder doesn't exist, so mkdir first.
        # mkdir will not overwrite existing folders by default. 
        # -p flag will prevent erroring if folder exists.
        command = <<-EOT
            mkdir -p ./../data/src_extract
            mkdir -p ./../data/src_transform
            mkdir -p ./../data/src_load
            cp -r ./../src/extract.py ./../data/src_extract/extract.py
            cp -r ./../src/transform.py ./../data/src_transform/transform.py
            cp -r ./../src/load.py ./../data/src_load/load.py
            EOT
    }

    triggers = {
        src_folder_hash = data.archive_file.src_zip.output_md5
    }
}

# resource "null_resource" "copy_src_extract" {
#     provisioner "local-exec" {
#         # cp will not work if folder doesn't exist, so mkdir first.
#         # mkdir will not overwrite existing folders. -p flag will
#         # prevent erroring if folder exists.
#         command = <<-EOT
#             mkdir -p ./../data/src_extract
#             cp -r ./../src/extract.py ./../data/src_extract/extract.py
#             EOT
#     }

#     triggers = {
#         src_file_hash = filemd5("./../src/extract.py")
#     }
# }

# resource "null_resource" "copy_src_transform" {
#     provisioner "local-exec" {
#         command = <<-EOT
#             mkdir -p ./../data/src_transform
#             cp -r ./../src/transform.py ./../data/src_transform/transform.py
#             EOT
#     }

#     triggers = {
#         src_file_hash = filemd5("./../src/transform.py")
#     }
# }

# resource "null_resource" "copy_src_load" {
#     provisioner "local-exec" {
#         command = <<-EOT
#             mkdir -p ./../data/src_load
#             cp -r ./../src/load.py ./../data/src_load/load.py
#             EOT
#     }

#     triggers = {
#         src_file_hash = filemd5("./../src/load.py")
#     }
# }
