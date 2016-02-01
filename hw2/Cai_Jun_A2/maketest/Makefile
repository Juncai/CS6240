test_env=
test:
ifeq (${TEST_ENV},)
	test_env=$(st)
else
	test_env=${TEST_ENV}
endif

test2: test
	$(test_env)
