from torch.utils.data import Dataset

from alluxio.alluxio_client import AlluxioClient


class AlluxioDataset(Dataset):
    def __init__(self, folder_path: str, client: AlluxioClient):
        self.client = client
        data = self.client.listdir(folder_path)
        file_paths = [item['name'] for item in data if item.get('type') == 'file']
        self.files = file_paths

    def __len__(self):
        return len(self.files)

    def __getitem__(self, idx):
        return self.client.read(self.files[idx])
