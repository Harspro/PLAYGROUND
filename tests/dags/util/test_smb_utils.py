from unittest.mock import patch
from util.smb_utils import SMBUtil


@patch('util.smb_utils.mkdir')
@patch('util.smb_utils.isdir')
def test_make_dir_recursively(mock_isdir, mock_mkdir):
    smbutil = SMBUtil('10.0.0.1', 'testuser', 'testpasswd')
    destinaction_folder = '/DFS001/folder1/folder2/'

    mock_isdir.return_value = False
    smbutil.make_dir_recursively(destinaction_folder)

    assert mock_isdir.call_count == 4
    assert mock_mkdir.call_count == 3

    destinaction_folder = '/DFS001/folder1/folder2'
    smbutil.make_dir_recursively(destinaction_folder)

    assert mock_isdir.call_count == 8
    assert mock_mkdir.call_count == 6
