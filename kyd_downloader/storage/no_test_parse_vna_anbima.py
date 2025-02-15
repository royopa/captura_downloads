from lxml import etree


def get_all_node_text(node):
    return ''.join(x.strip() for x in node.itertext())


def parse_vna_node(tree, id):
    trs = tree.xpath(f"//div[@id='{id}']/*/table/tr")
    instrument_ref = get_all_node_text(trs[0])
    date = get_all_node_text(trs[1][1])
    index_ref = get_all_node_text(trs[2][1])
    value = get_all_node_text(trs[3][1])
    rate_value = get_all_node_text(trs[3][2])
    try:
        projection = get_all_node_text(trs[3][3])
        rate_date = get_all_node_text(trs[3][4])
    except IndexError:
        projection = ''
        rate_date = ''
    return {
        'date': date,
        'value': value,
        'rate': rate_value,
        'rate_date': rate_date,
        'proj': projection,
        'index_ref': index_ref,
        'instrument_ref': instrument_ref,
    }


with open('data/2022-01-20.html', 'r', encoding='latin1') as fp:
    parser = etree.HTMLParser()
    tree = etree.parse(fp, parser)
    # sel = "div#listaNTN-B > center > table > tr:nth-child(2) > td:nth-child(2)"
    # date = root.cssselect(sel)[0].text_content()
    # sel = "div#listaNTN-B > center > table > tr:nth-child(4) > td:nth-child(2)"
    # value = root.cssselect(sel)[0].text_content()
    print(parse_vna_node(tree, 'listaNTN-B'))
    print(parse_vna_node(tree, 'listaNTN-C'))
    print(parse_vna_node(tree, 'listaLFT'))
