package com.ordersystem.service;

import com.ordersystem.dto.AuthRequest;
import com.ordersystem.dto.AuthResponse;
import com.ordersystem.dto.RegisterRequest;
import com.ordersystem.entity.Role;
import com.ordersystem.entity.User;
import com.ordersystem.enums.RoleType;
import com.ordersystem.exception.BusinessException;
import com.ordersystem.repository.RoleRepository;
import com.ordersystem.repository.UserRepository;
import com.ordersystem.security.JwtTokenProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthService {

    private final UserRepository userRepository;
    private final RoleRepository roleRepository;
    private final PasswordEncoder passwordEncoder;
    private final AuthenticationManager authenticationManager;
    private final JwtTokenProvider jwtTokenProvider;

    @Transactional
    public AuthResponse register(RegisterRequest request) {
        if (userRepository.existsByUsername(request.getUsername())) {
            throw new BusinessException("Username already exists");
        }
        if (userRepository.existsByEmail(request.getEmail())) {
            throw new BusinessException("Email already exists");
        }

        Role userRole = roleRepository.findByName(RoleType.USER)
                .orElseThrow(() -> new BusinessException("Default role not found"));

        User user = User.builder()
                .username(request.getUsername())
                .email(request.getEmail())
                .password(passwordEncoder.encode(request.getPassword()))
                .enabled(true)
                .roles(Set.of(userRole))
                .build();

        user = userRepository.save(user);
        log.info("User registered: {}", user.getUsername());

        String token = jwtTokenProvider.generateToken(user.getUsername());
        String roleName = user.getRoles().stream().findFirst().map(r -> r.getName().name()).orElse("USER");

        return new AuthResponse(token, user.getId(), user.getUsername(), user.getEmail(), roleName);
    }

    @Transactional(readOnly = true)
    public AuthResponse login(AuthRequest request) {
        Authentication authentication = authenticationManager.authenticate(
                new UsernamePasswordAuthenticationToken(request.getUsernameOrEmail(), request.getPassword())
        );

        String token = jwtTokenProvider.generateToken(authentication);
        User user = userRepository.findByUsername(request.getUsernameOrEmail())
                .or(() -> userRepository.findByEmail(request.getUsernameOrEmail()))
                .orElseThrow(() -> new BusinessException("User not found"));

        String roleName = user.getRoles().stream().findFirst().map(r -> r.getName().name()).orElse("USER");

        log.info("User logged in: {}", user.getUsername());
        return new AuthResponse(token, user.getId(), user.getUsername(), user.getEmail(), roleName);
    }
}
