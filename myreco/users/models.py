# MIT License

# Copyright (c) 2016 Diogo Dutra <dutradda@gmail.com>

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from swaggerit.utils import get_swagger_json
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
from base64 import b64decode
import sqlalchemy as sa
import binascii
import re


class GrantsModelBase(AbstractConcreteBase):
    __tablename__ = 'grants'
    __use_redis__ = False

    @declared_attr
    def uri_id(cls):
        return sa.Column(sa.ForeignKey('uris.id'), primary_key=True)

    @declared_attr
    def method_id(cls):
        return sa.Column(sa.ForeignKey('methods.id'), primary_key=True)

    @declared_attr
    def uri(cls):
        return sa.orm.relationship('URIsModel')

    @declared_attr
    def method(cls):
        return sa.orm.relationship('MethodsModel')


class URIsModelBase(AbstractConcreteBase):
    __tablename__ = 'uris'
    __use_redis__ = False

    id = sa.Column(sa.Integer, primary_key=True)
    uri = sa.Column(sa.String(255), unique=True, nullable=False)


class MethodsModelBase(AbstractConcreteBase):
    __tablename__ = 'methods'
    __use_redis__ = False

    id = sa.Column(sa.Integer, primary_key=True)
    method = sa.Column(sa.String(10), unique=True, nullable=False)


class UsersModelBase(AbstractConcreteBase):
    __tablename__ = 'users'
    __swagger_json__ = get_swagger_json(__file__)

    id = sa.Column(sa.String(255), primary_key=True)
    name = sa.Column(sa.String(255), unique=True, nullable=False)
    email = sa.Column(sa.String(255), unique=True, nullable=False)
    password = sa.Column(sa.String(255), nullable=False)
    admin = sa.Column(sa.Boolean, default=False)

    @declared_attr
    def grants(cls):
        grants_primaryjoin = 'UsersModel.id == users_grants.c.user_id'
        grants_secondaryjoin = 'and_('\
            'GrantsModel.uri_id == users_grants.c.grant_uri_id, '\
            'GrantsModel.method_id == users_grants.c.grant_method_id)'

        return sa.orm.relationship(
            'GrantsModel', uselist=True, secondary='users_grants',
            primaryjoin=grants_primaryjoin, secondaryjoin=grants_secondaryjoin)

    @declared_attr
    def stores(cls):
        return sa.orm.relationship('StoresModel', uselist=True, secondary='users_stores')

    @classmethod
    async def authorize(cls, session, authorization, url, method):
        try:
            authorization = b64decode(authorization).decode()
        except binascii.Error:
            return None
        except UnicodeDecodeError:
            return None

        if not ':' in authorization:
            return None

        user = await cls.get(session, {'id': authorization})
        user = user[0] if user else user
        if user and user.get('admin'):
            session.user = user
            return True

        elif user:
            if method == 'OPTIONS':
                return True

            for grant in user['grants']:
                grant_uri = grant['uri']['uri']
                if (grant_uri == url or re.match(grant_uri, url)) \
                        and grant['method']['method'].lower() == method.lower():
                    session.user = user
                    return True

            return False

    @classmethod
    async def insert(cls, session, objs, commit=True, todict=True):
        objs = cls._to_list(objs)
        await cls._set_objs_ids_and_grant(objs, session)
        return await type(cls).insert(cls, session, objs, commit, todict)

    @classmethod
    async def _set_objs_ids_and_grant(cls, objs, session):
        objs = cls._to_list(objs)

        patch_method = await cls.get_model('methods').get(session, ids={'method': 'patch'}, todict=False)
        if not patch_method:
            patch_method = await cls.get_model('methods').insert(session, [{'method': 'patch'}], todict=False)
        patch_method = patch_method[0]

        get_method = await cls.get_model('methods').get(session, ids={'method': 'get'}, todict=False)
        if not get_method:
            get_method = await cls.get_model('methods').insert(session, [{'method': 'get'}], todict=False)
        get_method = get_method[0]

        for obj in objs:
            new_grants = []
            user_uri = '/users/{}'.format(obj['email'])

            uri = await cls.get_model('uris').get(session, ids={'uri': user_uri}, todict=False)
            if not uri:
                uri = await cls.get_model('uris').insert(session, [{'uri': user_uri}], todict=False)
            uri = uri[0]

            grant = await cls.get_model('grants').get(session, {'uri_id': uri.id, 'method_id': patch_method.id}, todict=False)
            if grant:
                grant = grant[0].todict()
            else:
                grant = {'uri_id': uri.id, 'method_id': patch_method.id, '_operation': 'insert'}
            new_grants.append(grant)

            grant = await cls.get_model('grants').get(session, {'uri_id': uri.id, 'method_id': get_method.id}, todict=False)
            if grant:
                grant = grant[0].todict()
            else:
                grant = {'uri_id': uri.id, 'method_id': get_method.id, '_operation': 'insert'}
            new_grants.append(grant)

            obj['id'] = '{}:{}'.format(obj['email'], obj['password'])
            grants = obj.get('grants', [])
            grants.extend(new_grants)
            obj['grants'] = grants

    @classmethod
    async def update(cls, session, objs, commit=True, todict=True, ids=None, ids_keys=None):
        if not ids:
            ids = []
            objs = cls._to_list(objs)
            for obj in objs:
                id_ = obj.get('id')
                email = obj.get('email')
                if id_ is not None:
                    ids.append({'id': id_})
                    ids_keys = ('id',)
                elif email is not None:
                    ids.append({'email': email})
                    ids_keys = ('email',)

        insts = await type(cls).update(cls, session, objs, commit=False,
                            todict=False, ids=ids, ids_keys=ids_keys)
        cls._set_insts_ids(insts)

        if commit:
            await session.commit()
        return cls._build_todict_list(insts) if todict else insts

    @classmethod
    def _set_insts_ids(cls, insts):
        insts = cls._to_list(insts)
        for inst in insts:
            inst.id = '{}:{}'.format(inst.email, inst.password)


def build_users_grants_table(metadata, **kwargs):
    return sa.Table(
        'users_grants', metadata,
        sa.Column('user_id', sa.String(255), sa.ForeignKey('users.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        sa.Column('grant_uri_id', sa.Integer, sa.ForeignKey('grants.uri_id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        sa.Column('grant_method_id', sa.Integer, sa.ForeignKey('grants.method_id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        **kwargs)


def build_users_stores_table(metadata, **kwargs):
    return sa.Table(
        'users_stores', metadata,
        sa.Column('user_id', sa.String(255), sa.ForeignKey('users.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        sa.Column('store_id', sa.Integer, sa.ForeignKey('stores.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        **kwargs)
