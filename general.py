import os


def create_project(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def create_data(project_name, base_url):
    queue = project_name + '/queue.txt'
    crawled = project_name + '/crawled.txt'

    if not os.path.isfile(queue):
        write_file(queue, base_url)
    if not os.path.isfile(crawled):
        write_file(crawled, ' ')


def write_file(path, data):
    f = open(path, 'w')
    f.write(data)
    f.close()


def append_to_file(path, data):
    with open(path, 'a', encoding='utf-8') as file:
        file.write(data + '\n')


def delete_file_data(path):
    with open(path, 'w'):
        pass


def file_to_set(file_name):
    results = set()
    with open(file_name, 'rt', encoding='utf-8') as f:
        for line in f:
            results.add(line.replace('\n', ''))
    return results


def set_to_file(links, file):
    delete_file_data(file)
    for link in sorted(links):
        append_to_file(file, link)
