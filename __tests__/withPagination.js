const path = require('path');
const Sequelize = require('sequelize');

const withPagination = require('../src');

const sequelize = new Sequelize('test', null, null, {
  dialect: 'sqlite',
  storage: path.join(__dirname, 'db.sqlite'),
  logging: false,
});

let Test;
let Person;

const generateTestData = () => {
  Promise.all([
    Person.create({ name: 'john', id: 3, extra: 3 }),
    Person.create({ name: 'bob', id: 2, extra: 4 }),
    Person.create({ name: 'jony', id: 5, extra: 3 }),
    Person.create({ name: 'jacky', id: 1, extra: 4 }),
    Person.create({ name: 'little', id: 4, extra: 3 }),
  ]);

  return Promise.all([
    Test.create({ counter: 3, id: 1, extra: 4, personId: 4, isValid: true }),
    Test.create({ counter: 4, id: 2, extra: 4, personId: 2, isValid: false}),
    Test.create({ counter: 4, id: 3, extra: 3, personId: 1, isValid: null }),
    Test.create({ counter: 2, id: 4, extra: 3, personId: 5, isValid: false }),
    Test.create({ counter: 1, id: 5, extra: 3, personId: 3, isValid: null }),
  ]);
};

beforeEach(async () => {
  Person = sequelize.define('person', {
    id: { type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true },
    extra: Sequelize.INTEGER,
    name: Sequelize.STRING,
  });

  Test = sequelize.define('test', {
    id: { type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true },
    counter: Sequelize.INTEGER,
    extra: Sequelize.INTEGER,
    personId: {
      type: Sequelize.INTEGER,
      allowNull: true,
      references: { model: Person, key: 'id' },
    },
    isValid: Sequelize.BOOLEAN,
  });

  Person.hasMany(Test, { foreignKey: 'personId' });

  Test.belongsTo(Person, {
    foreignKey: 'personId',
    as: 'persons',
  });

  withPagination()(Test);
  withPagination()(Person);

  await sequelize.sync({ force: true });
});

const expectCorrectPageInfoCursors = (result) => {
  expect(typeof result.pageInfo.startCursor === 'string').toBe(true);
  expect(result.pageInfo.startCursor).toBe(result.edges[0].cursor);

  expect(typeof result.pageInfo.endCursor === 'string').toBe(true);

  expect(result.pageInfo.endCursor).toBe(
    result.edges[result.edges.length - 1].cursor,
  );
};

const expectIdsToEqual = (result, ids) => {
  expect(result.edges.map(({ node }) => node.id)).toEqual(ids);
};

test('sets correct default method', () => {
  expect(typeof Test.paginate === 'function').toBe(true);
});

test('sets correct method when methodName is provided', () => {
  withPagination({ methodName: 'myPaginate' })(Test);

  expect(typeof Test.myPaginate === 'function').toBe(true);
});

test('paginates correctly with after cursor', async () => {
  await generateTestData();

  let result = await Test.paginate({ limit: 2 });

  expectIdsToEqual(result, [1, 2]);
  expect(result.pageInfo.hasNextPage).toBe(true);
  expect(result.pageInfo.hasPreviousPage).toBe(false);
  expectCorrectPageInfoCursors(result);
  expect(result.totalCount).toBe(5);

  result = await Test.paginate({
    limit: 2,
    after: result.pageInfo.endCursor,
  });

  expectIdsToEqual(result, [3, 4]);
  expect(result.pageInfo.hasNextPage).toBe(true);
  expect(result.pageInfo.hasPreviousPage).toBe(true);
  expectCorrectPageInfoCursors(result);
  expect(result.totalCount).toBe(5);

  result = await Test.paginate({
    limit: 2,
    after: result.pageInfo.endCursor,
  });

  expectIdsToEqual(result, [5]);
  expect(result.pageInfo.hasNextPage).toBe(false);
  expect(result.pageInfo.hasPreviousPage).toBe(true);
  expectCorrectPageInfoCursors(result);
  expect(result.totalCount).toBe(5);
});

test('paginates correctly with before cursor', async () => {
  await generateTestData();

  let result = await Test.paginate({ limit: 2 });

  expectIdsToEqual(result, [1, 2]);

  result = await Test.paginate({
    limit: 2,
    after: result.pageInfo.endCursor,
  });

  expectIdsToEqual(result, [3, 4]);

  result = await Test.paginate({
    limit: 2,
    before: result.pageInfo.startCursor,
  });

  expectIdsToEqual(result, [1, 2]);
});

test('paginates correctly with simple order', async () => {
  await generateTestData();

  const order = [['counter', 'asc']];

  let result = await Test.paginate({ order, limit: 3 });

  expectIdsToEqual(result, [5, 4, 1]);

  result = await Test.paginate({
    order,
    limit: 3,
    after: result.pageInfo.endCursor,
  });

  expectIdsToEqual(result, [2, 3]);

  result = await Test.paginate({
    order,
    limit: 3,
    before: result.pageInfo.startCursor,
  });

  expectIdsToEqual(result, [5, 4, 1]);
});

test('paginates correctly with complex order', async () => {
  await generateTestData();

  const order = [
    ['counter', 'desc'],
    ['extra', 'asc'],
  ];

  let result = await Test.paginate({
    order,
    limit: 3,
  });

  expectIdsToEqual(result, [3, 2, 1]);

  result = await Test.paginate({
    order,
    limit: 3,
    after: result.pageInfo.endCursor,
  });

  expectIdsToEqual(result, [4, 5]);

  result = await Test.paginate({
    order,
    limit: 3,
    before: result.pageInfo.startCursor,
  });

  expectIdsToEqual(result, [3, 2, 1]);
});

test('paginates correctly with where', async () => {
  await generateTestData();

  const order = [['counter', 'asc']];

  let result = await Test.paginate({ order, where: { extra: 3 }, limit: 5 });

  expectIdsToEqual(result, [5, 4, 3]);
  expect(result.totalCount).toBe(3);
});

test('paginates correctly with boolean columns desc', async () => {
  await generateTestData();

  const order = [['isValid', 'DESC NULLS FIRST']];

  let result = await Test.paginate({ order, limit: 5 });

  // expecting order - [fnull -> true -> false] -> [ordered by id asc]
  expectIdsToEqual(result, [3,5,1,2,4]);
  expect(result.totalCount).toBe(5);
});

test('paginates correctly with boolean asc', async () => {
  await generateTestData();

  const order = [['isValid', 'ASC NULLS LAST']];

  let result = await Test.paginate({ order, limit: 5 });

  // expecting order - [false -> true -> null] -> [ordered by id asc]
  expectIdsToEqual(result, [2, 4, 1, 3, 5]);
  expect(result.totalCount).toBe(5);
});

test('paginates correctly with boolean asc', async () => {
  await generateTestData();

  const order = [['isValid', 'DESC NULLS LAST']];

  let result = await Test.paginate({ order, limit: 5 });

  // expecting order - [true -> false -> null] -> [ordered by id desc nulls last]
  expectIdsToEqual(result, [1, 2, 4, 3, 5]);
  expect(result.totalCount).toBe(5);
});

test('paginates correctly with boolean asc', async () => {
  await generateTestData();

  const order = [['isValid', 'DESC NULLS FIRST']];

  let result = await Test.paginate({ order, limit: 5 });

  // expecting order - [null -> true -> false] -> [ordered by id desc nulls first]
  expectIdsToEqual(result, [3, 5, 1, 2, 4]);
  expect(result.totalCount).toBe(5);
});

test('paginates correctly with different order formats', async () => {
  await generateTestData();

  let result = await Test.paginate({ order: [['counter']], limit: 5 });

  expectIdsToEqual(result, [5, 4, 1, 2, 3]);

  result = await Test.paginate({ order: [['counter', 'asc']], limit: 5 });

  expectIdsToEqual(result, [5, 4, 1, 2, 3]);

  result = await Test.paginate({ order: [['counter', 'desc']], limit: 5 });

  expectIdsToEqual(result, [2, 3, 1, 4, 5]);
});

test('paginates correctly with order only with Associated model', async () => {
  await generateTestData();

  const order = [[{ model: Person, as: 'persons' }, 'name', 'asc']];

  const where = {
    include: [
      {
        model: Person,
        as: 'persons',
      },
    ],
  };

  let result = await Test.paginate({
    ...where,
    order,
    limit: 3,
  });

  expectIdsToEqual(result, [2, 3, 5]);

  result = await Test.paginate({
    ...where,
    order,
    limit: 3,
    after: result.pageInfo.endCursor,
  });

  expectIdsToEqual(result, [4, 1]);

  result = await Test.paginate({
    ...where,
    order,
    limit: 3,
    before: result.pageInfo.startCursor,
  });

  expectIdsToEqual(result, [2, 3, 5]);
});

test('paginates correctly with complex order on associated model', async () => {
  await generateTestData();

  const order = [
    ['counter', 'desc'],
    ['extra', 'asc'],
    [{ model: Person, as: 'persons' }, 'name', 'asc'],
  ];

  const where = {
    include: [
      {
        model: Person,
        as: 'persons',
      },
    ],
  };

  let result = await Test.paginate({
    ...where,
    order,
    limit: 3,
  });

  expectIdsToEqual(result, [3, 2, 1]);

  result = await Test.paginate({
    ...where,
    order,
    limit: 3,
    after: result.pageInfo.endCursor,
  });

  expectIdsToEqual(result, [4, 5]);

  result = await Test.paginate({
    ...where,
    order,
    limit: 3,
    before: result.pageInfo.startCursor,
  });

  expectIdsToEqual(result, [3, 2, 1]);
});
