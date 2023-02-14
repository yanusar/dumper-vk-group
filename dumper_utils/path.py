def get_dname_for_data(owner_id: int) -> str:
    return "data_club{}".format(-owner_id)


def get_fname_for_method_result(owner_id: int, method_name: str) -> str:
    return "club{}_{}.json".format(-owner_id, method_name.replace(".", "_"))
