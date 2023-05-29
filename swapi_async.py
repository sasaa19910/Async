import asyncio
import datetime
import aiohttp

from more_itertools import chunked
from models import engine, Session, Base, SwapiPeople


async def get_people(people_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.dev/api/people/{people_id}")
    json_data = await response.json()
    await session.close()
    return json_data


async def pasted_to_db(persons_json):
    async with Session() as session:
        orm_obj = [SwapiPeople(json=item) for item in persons_json]
        session.add_all(orm_obj)
        await session.commit()


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    person_coros = (get_people(i) for i in range(1, 86))

    person_coro_chunked = chunked(person_coros, 5)

    for person_coro_chunke in person_coro_chunked:
        persons = await asyncio.gather(*person_coro_chunke)
        asyncio.create_task(pasted_to_db(persons))
        print(persons)

    tasks = asyncio.all_tasks() - {asyncio.current_task(), }
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)

