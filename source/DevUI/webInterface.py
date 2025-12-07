import asyncio
from aiohttp import web
from hashlib import sha256
from base64 import b64encode, b64decode
from dataclasses import dataclass
from Deprecated import Logger, Profiler, CoreMultiprocessing, Task
import time

web_app_logger = Logger("DevUI", "network.log")

@dataclass
class DeveloperRecord:
    username: str
    password: str # hashed password
    token: str
class JWT:
    def __init__(self, secret: str):
        self.secret = secret
        self.tokens = {}

    def create_one(self, claims: dict) -> str:
        """
        Creates a JWT token with the given claims.
        """
        header = {"alg": "HS256", "typ": "JWT"}
        header_encoded = b64encode(str(header).encode()).hex().upper()
        claims_encoded = b64encode(str(claims).encode()).hex().upper()
        signature = sha256(f"{header_encoded}.{claims_encoded}.{self.secret}".encode()).hexdigest()
        token = f"{header_encoded}.{claims_encoded}.{signature}"
        self.tokens[token] = token
        return token

    def validate(self, token: str) -> bool:
        """
        Validates the JWT token.
        """
        if token not in self.tokens:
            return False
        header, claims, signature = token.split('.')

        # Check expiration
        parsed_claims: dict = eval(b64decode(claims).decode())
        if 'expires_at' in parsed_claims and time.time() > float(parsed_claims['expires_at']):
            return False
        # Check signature
        expected_signature = sha256(f"{header}.{claims}.{self.secret}".encode()).hexdigest()
        return expected_signature == signature

    def get_claims(self, token: str) -> dict:
        """
        Returns the claims from the JWT token.
        """
        if not self.validate(token):
            return {}
        header, claims, signature = token.split('.')
        claims = b64decode(claims.encode()).decode()
        return eval(claims)


class WebInterface:
    def __init__(self, secret: str, host: str, port: int):
        self.app = web.Application(
            middlewares=[
                self.auth_middleware,
            ]
        )
        self.jwt = JWT(secret)
        self.secret = secret
        self.users = []
        self.profiler = Profiler.get_instance()
        self._processes_stats = {}
        self._system_stats = {}
        self.setup_routes()
        self.loop_task = Task(
            func=self._profiler_polling,
            name="WebInterface._profiler_polling"
        )
        CoreMultiprocessing.push_task(self.loop_task)
        web.run_app(self.app, host=host, port=port)

    async def _profiler_polling(self):
        while True:
            self._system_stats = self.profiler.get_system_stats()
            self._processes_stats = self.profiler.get_process_stats()
            self._system_stats["Load Average"] = self.profiler.load_average
            await asyncio.sleep(1)


    def setup_routes(self):
        self.app.router.add_get('/', self.index_get)
        self.app.router.add_post('/', self.index_post)
        self.app.router.add_get('/login', self.loging_get)
        self.app.router.add_post('/login', self.login_post)
        self.app.router.add_get('/signup', self.signup_get)
        self.app.router.add_post('/signup', self.signup_post)
        self.app.router.add_get('/system_stats', self.get_system_stats)
        self.app.router.add_get('/processes_stats', self.get_processes_stats)



    @web.middleware
    async def auth_middleware(self, request: web.Request, handler) -> web.Response:
        if request.path in ('/login', '/signup'):
            return await handler(request)

        session_token = request.cookies.get('session_token')
        if session_token and self.jwt.validate(session_token):
            return await handler(request)

        token = request.headers.get("Authorization")
        if not token or not self.jwt.validate(token.split('Bearer ')[-1]):
            raise web.HTTPFound('/login')

        await web_app_logger.info(f"User {self.jwt.get_claims(token.split('Bearer ')[-1])['username']} logged in")

        return await handler(request)

    async def loging_get(self, request: web.Request) -> web.Response:
        """
        Handles the login request.
        """
        with open('source/DevUI/login.html', 'r') as f:
            return web.Response(text=f.read(), content_type='text/html')

    async def signup_get(self, request: web.Request) -> web.Response:
        """
        Handles the signup request.
        """
        with open('source/DevUI/signup.html', 'r') as f:
            return web.Response(text=f.read(), content_type='text/html')

    async def signup_post(self, request: web.Request) -> web.Response:
        """
        Handles the signup request.
        """
        data = await request.post()
        username = data.get('username')
        password = data.get('password')

        if len(self.users) == 4:
            return web.Response(text='Max users reached', status=403)

        if not username and not password:
            return web.Response(text='Invalid input', status=400)

        hashed_password = sha256((password + self.secret).encode()).hexdigest()
        claims = {
            'username': username,
            'password': hashed_password,
            'expires_at': str(time.time() + 60 * 60 * 24),
        }
        new_token = self.jwt.create_one(claims)
        # Since we are not storing developer records in a database, we will just keep it in memory. Since there are two devs
        self.users.append(DeveloperRecord(username, hashed_password, new_token))
        await web_app_logger.info(f"User {username} created successfully")
        response = web.HTTPFound('/')
        # Set the token in the response
        response.headers['Authorization'] = f'Bearer {new_token}'
        response.set_cookie('session_token', new_token)
        # Return the token in the response
        return response


    async def login_post(self, request: web.Request) -> web.Response:
        """
        Handles the login request.
        """
        data = await request.post()
        username = data.get('username')
        password = data.get('password')

        for record in self.users:
            if record.username != username:
                continue
            if record.password != password:
                continue
            if not self.jwt.validate(record.token):
                record.token = self.jwt.create_one(
                    {
                        'username': username,
                        'password': sha256((password + self.secret).encode()).hexdigest(),
                        'expires_at': str(time.time() + 60 * 60 * 24),
                    }
                )
            response = web.HTTPFound('/')
            response.headers['Authorization'] = f'Bearer {record.token}'
            response.set_cookie('session_token', record.token)
            return response

        return web.Response(text='Invalid credentials', status=401)

    async def index_get(self, request: web.Request) -> web.Response:
        """
        Handles the index request.
        """
        with open('source/DevUI/index.html', 'r') as f:
            return web.Response(text=f.read(), content_type='text/html')

    async def index_post(self, request: web.Request) -> web.Response:
        """
        Handles the index post request.
        """
        data = await request.post()
        if data.get('action') == 'logout':
            raise web.HTTPFound('/login')
        cookie = request.cookies.get('session_token')
        for record in self.users:
            if record.token == cookie:
                if data.get('action') == 'logout':
                    record.token = None
                    response = web.HTTPFound('/login')
                    response.set_cookie('session_token', '', expires=0)
                    return response

        return web.Response(text='Invalid action', status=400)

    async def get_system_stats(self, request: web.Request) -> web.Response:
        """
        Returns the system stats.
        """
        return web.json_response(self._system_stats)

    async def get_processes_stats(self, request: web.Request) -> web.Response:
        """
        Returns the processes stats.
        """
        return web.json_response(self._processes_stats)

    async def stop(self):
        """
        Stops the web interface.
        """
        await self.app.shutdown()
        await self.app.cleanup()
