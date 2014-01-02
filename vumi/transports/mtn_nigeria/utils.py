from twisted.web import microdom


def is_xml_leaf(el):
    if len(el.childNodes) == 1:
        return isinstance(el.firstChild(), microdom.Text)

    return len(el.childNodes) == 0


def xml_leaf_text(el):
    if len(el.childNodes) == 0:
        return ''

    text = el.firstChild()
    return text.value.strip()


def xml_to_collection(el):
    if is_xml_leaf(el):
        return (el.nodeName, xml_leaf_text(el))

    children = [xml_to_collection(child) for child in el.childNodes]
    return (el.nodeName, children)


def collection_to_xml(name, contents):
    el = microdom.Element(name.encode('utf8'), preserveCase=True)

    if isinstance(contents, list):
        for child_name, child_contents in contents:
            el.appendChild(collection_to_xml(child_name, child_contents))
    else:
        el.appendChild(microdom.Text(contents.encode('utf8')))

    return el


def xml_string_to_collection(data):
    document = microdom.parseXMLString(data)
    return xml_to_collection(document.firstChild())


def collection_to_xml_string(name, contents):
    return collection_to_xml(name, contents).toxml()
