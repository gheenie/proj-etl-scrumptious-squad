resource "null_resource" "install_dependencies" {
    provisioner "local-exec" {
        command = <<-EOT
            rm -r ./../data/src
            cp -r ./../src ./../data/src
            pip install pg8000 -t ./../data/src
            pip install python-dotenv -t ./../data/src
            EOT
    }

    triggers = {
        dependencies_versions = filemd5("./../requirements.txt")
    }
}
