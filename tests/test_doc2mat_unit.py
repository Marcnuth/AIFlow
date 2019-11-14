from aiflow.units.doc2mat_unit import Doc2MatUnit


def test_main():
    

    model = Doc2MatUnit(3, './tests/resources/crawl-300d-2M.vec')
    print(model.execute(sentence='This is from facebook, IT, information, google, company who knows this is'))
    print(model.execute(sentence=''))
    print(model.execute(sentence='facebook, google, apple'))
    print(model.execute(sentence='facebook'))
